package spannerserver

import (
	"context"
	"net"

	"github.com/uji/go-spanner-server/service"

	lropb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	databasepb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

// Server is an in-process, in-memory Cloud Spanner compatible server.
type Server struct {
	lis *bufconn.Listener
	srv *grpc.Server
}

// New creates and starts a new Server.
func New() *Server {
	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer()

	sessions := service.NewSessionManager()
	admin := service.NewDatabaseAdminServer()
	spannerSvc := service.NewSpannerServer(sessions, admin)
	opsSvc := service.NewOperationsServer(admin)

	sppb.RegisterSpannerServer(srv, spannerSvc)
	databasepb.RegisterDatabaseAdminServer(srv, admin)
	lropb.RegisterOperationsServer(srv, opsSvc)

	go srv.Serve(lis)

	return &Server{
		lis: lis,
		srv: srv,
	}
}

// Stop gracefully stops the server.
func (s *Server) Stop() {
	s.srv.GracefulStop()
	s.lis.Close()
}

// Conn returns a gRPC client connection to the in-process server.
func (s *Server) Conn(ctx context.Context) (*grpc.ClientConn, error) {
	return grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return s.lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
}
