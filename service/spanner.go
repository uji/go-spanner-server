package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/uji/go-spanner-server/engine"
	"github.com/uji/go-spanner-server/store"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
)

// SpannerServer implements sppb.SpannerServer.
type SpannerServer struct {
	sppb.UnimplementedSpannerServer

	sessions *SessionManager
	admin    *DatabaseAdminServer
}

// NewSpannerServer creates a new SpannerServer.
func NewSpannerServer(sessions *SessionManager, admin *DatabaseAdminServer) *SpannerServer {
	return &SpannerServer{
		sessions: sessions,
		admin:    admin,
	}
}

func (s *SpannerServer) CreateSession(ctx context.Context, req *sppb.CreateSessionRequest) (*sppb.Session, error) {
	id := generateID()
	name := req.Database + "/sessions/" + id
	s.sessions.CreateSession(name)
	return &sppb.Session{Name: name}, nil
}

func (s *SpannerServer) BatchCreateSessions(ctx context.Context, req *sppb.BatchCreateSessionsRequest) (*sppb.BatchCreateSessionsResponse, error) {
	count := req.SessionCount
	if count <= 0 {
		count = 1
	}
	sessions := make([]*sppb.Session, count)
	for i := int32(0); i < count; i++ {
		id := generateID()
		name := req.Database + "/sessions/" + id
		s.sessions.CreateSession(name)
		sessions[i] = &sppb.Session{Name: name}
	}
	return &sppb.BatchCreateSessionsResponse{Session: sessions}, nil
}

func (s *SpannerServer) GetSession(ctx context.Context, req *sppb.GetSessionRequest) (*sppb.Session, error) {
	if _, ok := s.sessions.GetSession(req.Name); !ok {
		return nil, status.Errorf(codes.NotFound, "session %q not found", req.Name)
	}
	return &sppb.Session{Name: req.Name}, nil
}

func (s *SpannerServer) DeleteSession(ctx context.Context, req *sppb.DeleteSessionRequest) (*emptypb.Empty, error) {
	s.sessions.DeleteSession(req.Name)
	return &emptypb.Empty{}, nil
}

func (s *SpannerServer) BeginTransaction(ctx context.Context, req *sppb.BeginTransactionRequest) (*sppb.Transaction, error) {
	readOnly := false
	if req.Options != nil {
		if _, ok := req.Options.Mode.(*sppb.TransactionOptions_ReadOnly_); ok {
			readOnly = true
		}
	}

	tx, err := s.sessions.BeginTransaction(req.Session, readOnly)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}

	return &sppb.Transaction{Id: tx.ID}, nil
}

func (s *SpannerServer) Commit(ctx context.Context, req *sppb.CommitRequest) (*sppb.CommitResponse, error) {
	// Resolve database from session name
	db, err := s.getDBFromSession(req.Session)
	if err != nil {
		return nil, err
	}

	// Apply mutations
	for _, mut := range req.Mutations {
		switch op := mut.Operation.(type) {
		case *sppb.Mutation_Insert:
			if err := s.applyInsert(db, op.Insert); err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "insert: %v", err)
			}
		case *sppb.Mutation_InsertOrUpdate:
			if err := s.applyInsert(db, op.InsertOrUpdate); err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "insert_or_update: %v", err)
			}
		default:
			return nil, status.Errorf(codes.Unimplemented, "unsupported mutation type: %T", op)
		}
	}

	return &sppb.CommitResponse{}, nil
}

func (s *SpannerServer) applyInsert(db *store.Database, write *sppb.Mutation_Write) error {
	table, err := db.GetTable(write.Table)
	if err != nil {
		return err
	}

	cols := write.Columns
	for _, row := range write.Values {
		vals := make([]any, len(row.Values))
		for i, v := range row.Values {
			colIdx, ok := table.ColIndex[cols[i]]
			if !ok {
				return fmt.Errorf("column %q not found", cols[i])
			}
			decoded, err := store.DecodeValue(v, table.Cols[colIdx].Type)
			if err != nil {
				return fmt.Errorf("decode column %q: %w", cols[i], err)
			}
			vals[i] = decoded
		}
		if err := table.InsertRow(cols, vals); err != nil {
			return err
		}
	}
	return nil
}

func (s *SpannerServer) Rollback(ctx context.Context, req *sppb.RollbackRequest) (*emptypb.Empty, error) {
	// MVP: no-op
	return &emptypb.Empty{}, nil
}

func (s *SpannerServer) ExecuteSql(ctx context.Context, req *sppb.ExecuteSqlRequest) (*sppb.ResultSet, error) {
	db, err := s.getDBFromSession(req.Session)
	if err != nil {
		return nil, err
	}

	result, err := engine.Execute(db, req.Sql)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "execute SQL: %v", err)
	}

	rs := &sppb.ResultSet{
		Metadata: &sppb.ResultSetMetadata{
			RowType: &sppb.StructType{Fields: result.Columns},
		},
		Rows: result.Rows,
	}
	return rs, nil
}

func (s *SpannerServer) ExecuteStreamingSql(req *sppb.ExecuteSqlRequest, stream sppb.Spanner_ExecuteStreamingSqlServer) error {
	db, err := s.getDBFromSession(req.Session)
	if err != nil {
		return err
	}

	result, err := engine.Execute(db, req.Sql)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "execute SQL: %v", err)
	}

	// Send all results in one PartialResultSet
	metadata := &sppb.ResultSetMetadata{
		RowType: &sppb.StructType{Fields: result.Columns},
	}

	// Flatten rows into values
	var values []*structpb.Value
	for _, row := range result.Rows {
		values = append(values, row.Values...)
	}

	prs := &sppb.PartialResultSet{
		Metadata: metadata,
		Values:   values,
	}
	return stream.Send(prs)
}

func (s *SpannerServer) StreamingRead(req *sppb.ReadRequest, stream sppb.Spanner_StreamingReadServer) error {
	db, err := s.getDBFromSession(req.Session)
	if err != nil {
		return err
	}

	table, err := db.GetTable(req.Table)
	if err != nil {
		return status.Errorf(codes.NotFound, "%v", err)
	}

	colIndexes, err := table.ResolveColumnIndexes(req.Columns)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "%v", err)
	}

	// Build metadata
	fields := make([]*sppb.StructType_Field, len(req.Columns))
	for i, idx := range colIndexes {
		col := table.Cols[idx]
		fields[i] = &sppb.StructType_Field{
			Name: req.Columns[i],
			Type: &sppb.Type{Code: store.TypeCodeFromDDL(col.Type)},
		}
	}
	metadata := &sppb.ResultSetMetadata{
		RowType: &sppb.StructType{Fields: fields},
	}

	// Read rows based on KeySet
	var rows []store.Row
	if req.KeySet != nil {
		if req.KeySet.All {
			rows = table.ReadAll(colIndexes)
		} else if len(req.KeySet.Keys) > 0 {
			keys := make([][]any, len(req.KeySet.Keys))
			for i, k := range req.KeySet.Keys {
				keyVals := make([]any, len(k.Values))
				for j, v := range k.Values {
					pkColIdx := table.PKCols[j]
					decoded, err := store.DecodeValue(v, table.Cols[pkColIdx].Type)
					if err != nil {
						return status.Errorf(codes.InvalidArgument, "decode key: %v", err)
					}
					keyVals[j] = decoded
				}
				keys[i] = keyVals
			}
			rows = table.ReadByKeys(keys, colIndexes)
		} else if len(req.KeySet.Ranges) > 0 {
			for _, kr := range req.KeySet.Ranges {
				startKey, startClosed := decodeKeyRangeStart(kr, table)
				endKey, endClosed := decodeKeyRangeEnd(kr, table)
				rangeRows := table.ReadByRange(startKey, endKey, startClosed, endClosed, colIndexes)
				rows = append(rows, rangeRows...)
			}
		}
	}

	// Encode rows and send
	var values []*structpb.Value
	for _, row := range rows {
		for i, idx := range colIndexes {
			_ = i
			values = append(values, store.EncodeValue(row.Data[i], table.Cols[idx].Type))
		}
	}

	prs := &sppb.PartialResultSet{
		Metadata: metadata,
		Values:   values,
	}
	return stream.Send(prs)
}

func decodeKeyRangeStart(kr *sppb.KeyRange, table *store.Table) ([]any, bool) {
	switch s := kr.StartKeyType.(type) {
	case *sppb.KeyRange_StartClosed:
		return decodeKeyValues(s.StartClosed, table), true
	case *sppb.KeyRange_StartOpen:
		return decodeKeyValues(s.StartOpen, table), false
	default:
		return nil, true
	}
}

func decodeKeyRangeEnd(kr *sppb.KeyRange, table *store.Table) ([]any, bool) {
	switch e := kr.EndKeyType.(type) {
	case *sppb.KeyRange_EndClosed:
		return decodeKeyValues(e.EndClosed, table), true
	case *sppb.KeyRange_EndOpen:
		return decodeKeyValues(e.EndOpen, table), false
	default:
		return nil, true
	}
}

func decodeKeyValues(lv *structpb.ListValue, table *store.Table) []any {
	if lv == nil {
		return nil
	}
	vals := make([]any, len(lv.Values))
	for i, v := range lv.Values {
		pkColIdx := table.PKCols[i]
		decoded, _ := store.DecodeValue(v, table.Cols[pkColIdx].Type)
		vals[i] = decoded
	}
	return vals
}

func (s *SpannerServer) getDBFromSession(sessionName string) (*store.Database, error) {
	// Session name format: projects/P/instances/I/databases/D/sessions/S
	// Database name: projects/P/instances/I/databases/D
	parts := strings.Split(sessionName, "/sessions/")
	if len(parts) != 2 {
		return nil, status.Errorf(codes.InvalidArgument, "invalid session name: %s", sessionName)
	}
	dbName := parts[0]

	db, ok := s.admin.GetDB(dbName)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "database %q not found", dbName)
	}
	return db, nil
}

func generateID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}
