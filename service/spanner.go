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
			if err := s.applyReplace(db, op.InsertOrUpdate); err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "insert_or_update: %v", err)
			}
		case *sppb.Mutation_Update:
			if err := s.applyUpdate(db, op.Update); err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "update: %v", err)
			}
		case *sppb.Mutation_Replace:
			if err := s.applyReplace(db, op.Replace); err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "replace: %v", err)
			}
		case *sppb.Mutation_Delete_:
			if err := s.applyDelete(db, op.Delete); err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "delete: %v", err)
			}
		default:
			return nil, status.Errorf(codes.Unimplemented, "unsupported mutation type: %T", op)
		}
	}

	return &sppb.CommitResponse{}, nil
}

func (s *SpannerServer) decodeWrite(db *store.Database, write *sppb.Mutation_Write) (*store.Table, []string, [][]any, error) {
	table, err := db.GetTable(write.Table)
	if err != nil {
		return nil, nil, nil, err
	}

	cols := write.Columns
	decoded := make([][]any, len(write.Values))
	for i, row := range write.Values {
		vals := make([]any, len(row.Values))
		for j, v := range row.Values {
			colIdx, ok := table.ColIndex[cols[j]]
			if !ok {
				return nil, nil, nil, fmt.Errorf("column %q not found", cols[j])
			}
			d, err := store.DecodeValue(v, table.Cols[colIdx].Type)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("decode column %q: %w", cols[j], err)
			}
			vals[j] = d
		}
		decoded[i] = vals
	}
	return table, cols, decoded, nil
}

func (s *SpannerServer) applyInsert(db *store.Database, write *sppb.Mutation_Write) error {
	table, cols, rows, err := s.decodeWrite(db, write)
	if err != nil {
		return err
	}
	for _, vals := range rows {
		if err := db.InsertRow(table, cols, vals); err != nil {
			return err
		}
	}
	return nil
}

func (s *SpannerServer) applyUpdate(db *store.Database, write *sppb.Mutation_Write) error {
	table, cols, rows, err := s.decodeWrite(db, write)
	if err != nil {
		return err
	}
	for _, vals := range rows {
		if err := db.UpdateRow(table, cols, vals); err != nil {
			return err
		}
	}
	return nil
}

func (s *SpannerServer) applyReplace(db *store.Database, write *sppb.Mutation_Write) error {
	table, cols, rows, err := s.decodeWrite(db, write)
	if err != nil {
		return err
	}
	for _, vals := range rows {
		if err := db.ReplaceRow(table, cols, vals); err != nil {
			return err
		}
	}
	return nil
}

func (s *SpannerServer) applyDelete(db *store.Database, del *sppb.Mutation_Delete) error {
	table, err := db.GetTable(del.Table)
	if err != nil {
		return err
	}

	ks := del.KeySet
	if ks.All {
		return db.DeleteAll(table)
	}

	if len(ks.Keys) > 0 {
		keys := make([][]any, len(ks.Keys))
		for i, k := range ks.Keys {
			keyVals := make([]any, len(k.Values))
			for j, v := range k.Values {
				pkColIdx := table.PKCols[j]
				decoded, err := store.DecodeValue(v, table.Cols[pkColIdx].Type)
				if err != nil {
					return fmt.Errorf("decode key: %w", err)
				}
				keyVals[j] = decoded
			}
			keys[i] = keyVals
		}
		if err := db.DeleteByKeys(table, keys); err != nil {
			return err
		}
	}

	if len(ks.Ranges) > 0 {
		for _, kr := range ks.Ranges {
			startKey, startClosed := decodeKeyRangeStart(kr, table)
			endKey, endClosed := decodeKeyRangeEnd(kr, table)
			if err := db.DeleteByRange(table, startKey, endKey, startClosed, endClosed); err != nil {
				return err
			}
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

	if isDML(req.Sql) {
		// Handle inline begin: the client may embed BeginTransaction in the first RPC
		// to save a round-trip. Begin a transaction and return its ID so the client
		// does not treat the missing ID as a failure and retry.
		var inlineTxID []byte
		if sel, ok := req.Transaction.GetSelector().(*sppb.TransactionSelector_Begin); ok {
			readOnly := false
			if sel.Begin != nil {
				if _, ok := sel.Begin.Mode.(*sppb.TransactionOptions_ReadOnly_); ok {
					readOnly = true
				}
			}
			tx, err := s.sessions.BeginTransaction(req.Session, readOnly)
			if err != nil {
				return nil, status.Errorf(codes.NotFound, "%v", err)
			}
			inlineTxID = tx.ID
		}

		rowCount, err := engine.ExecuteDML(db, req.Sql)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "execute DML: %v", err)
		}
		rs := &sppb.ResultSet{
			Stats: &sppb.ResultSetStats{
				RowCount: &sppb.ResultSetStats_RowCountExact{RowCountExact: rowCount},
			},
		}
		if inlineTxID != nil {
			rs.Metadata = &sppb.ResultSetMetadata{
				Transaction: &sppb.Transaction{Id: inlineTxID},
			}
		}
		return rs, nil
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

// isDML reports whether sql is a DML statement (INSERT, UPDATE, DELETE).
func isDML(sql string) bool {
	fields := strings.Fields(strings.TrimSpace(sql))
	if len(fields) == 0 {
		return false
	}
	switch strings.ToUpper(fields[0]) {
	case "INSERT", "UPDATE", "DELETE":
		return true
	}
	return false
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
			Type: store.SpannerType(col.Type),
		}
	}
	metadata := &sppb.ResultSetMetadata{
		RowType: &sppb.StructType{Fields: fields},
	}

	// Read rows based on KeySet
	var rows []store.Row
	if req.Index != "" {
		rows, err = s.readUsingIndex(table, req, colIndexes)
		if err != nil {
			return err
		}
	} else if req.KeySet != nil {
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

func (s *SpannerServer) readUsingIndex(table *store.Table, req *sppb.ReadRequest, colIndexes []int) ([]store.Row, error) {
	idx, ok := table.Indexes[req.Index]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "index %q not found on table %q", req.Index, req.Table)
	}

	var rows []store.Row
	if req.KeySet != nil {
		if req.KeySet.All {
			rows = idx.ReadAll(table, colIndexes)
		} else if len(req.KeySet.Keys) > 0 {
			keys := make([][]any, len(req.KeySet.Keys))
			for i, k := range req.KeySet.Keys {
				keyVals := make([]any, len(k.Values))
				idxKeyCols := idx.IndexKeyCols()
				for j, v := range k.Values {
					colIdx := idxKeyCols[j]
					decoded, err := store.DecodeValue(v, table.Cols[colIdx].Type)
					if err != nil {
						return nil, status.Errorf(codes.InvalidArgument, "decode index key: %v", err)
					}
					keyVals[j] = decoded
				}
				keys[i] = keyVals
			}
			rows = idx.ReadByKeys(table, keys, colIndexes)
		} else if len(req.KeySet.Ranges) > 0 {
			idxKeyCols := idx.IndexKeyCols()
			for _, kr := range req.KeySet.Ranges {
				startKey, startClosed := decodeIndexKeyRangeStart(kr, table, idxKeyCols)
				endKey, endClosed := decodeIndexKeyRangeEnd(kr, table, idxKeyCols)
				rangeRows := idx.ReadByRange(table, startKey, endKey, startClosed, endClosed, colIndexes)
				rows = append(rows, rangeRows...)
			}
		}
	}
	return rows, nil
}

func decodeIndexKeyRangeStart(kr *sppb.KeyRange, table *store.Table, idxKeyCols []int) ([]any, bool) {
	switch s := kr.StartKeyType.(type) {
	case *sppb.KeyRange_StartClosed:
		return decodeIndexKeyValues(s.StartClosed, table, idxKeyCols), true
	case *sppb.KeyRange_StartOpen:
		return decodeIndexKeyValues(s.StartOpen, table, idxKeyCols), false
	default:
		return nil, true
	}
}

func decodeIndexKeyRangeEnd(kr *sppb.KeyRange, table *store.Table, idxKeyCols []int) ([]any, bool) {
	switch e := kr.EndKeyType.(type) {
	case *sppb.KeyRange_EndClosed:
		return decodeIndexKeyValues(e.EndClosed, table, idxKeyCols), true
	case *sppb.KeyRange_EndOpen:
		return decodeIndexKeyValues(e.EndOpen, table, idxKeyCols), false
	default:
		return nil, true
	}
}

func decodeIndexKeyValues(lv *structpb.ListValue, table *store.Table, idxKeyCols []int) []any {
	if lv == nil {
		return nil
	}
	vals := make([]any, len(lv.Values))
	for i, v := range lv.Values {
		if i >= len(idxKeyCols) {
			break
		}
		colIdx := idxKeyCols[i]
		decoded, _ := store.DecodeValue(v, table.Cols[colIdx].Type)
		vals[i] = decoded
	}
	return vals
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
