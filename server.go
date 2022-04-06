package solmul

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

func newServer(ctx context.Context, port int, server_mux *http.ServeMux) (*http.Server, context.CancelFunc) {
	new_base, cancel := context.WithCancel(ctx)
	return &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: server_mux,
		BaseContext: func(net.Listener) context.Context {
			return new_base
		},
	}, cancel
}

// RunServer launches servers for rpc and websockets in a separate go routine.
// - the servers can be shut down by cancelling the input context.
// - if rpc_port and ws_port is the same, only one single server will be launched.
// - returned <-chan bool can be used to wait on the process to finish.
func RunServer(ctx context.Context, rpc_urls, ws_urls []string, rpc_port, ws_port int) (<-chan struct{}, error) {
	if len(rpc_urls) == 0 {
		return nil, fmt.Errorf("rpc urls is nil")
	}
	rpc_handler := rpcCaller{Urls: rpc_urls}

	ws_handler, err := NewStreamMapper(ws_urls)
	if err != nil {
		return nil, err
	}

	// Launch web socket loop
	go ws_handler.MainLoop(ctx)

	server_mux := http.NewServeMux()

	server_mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		if websocket.IsWebSocketUpgrade(req) {
			ws_handler.RunDownstream(w, req)
		} else {
			rpc_handler.HandleRequest(w, req)
		}
	})

	rpc_server, rpc_cancel := newServer(ctx, rpc_port, server_mux)
	cancels := []context.CancelFunc{rpc_cancel}
	servers := []*http.Server{rpc_server}
	if rpc_port != ws_port {
		ws_server, ws_cancel := newServer(ctx, ws_port, server_mux)
		cancels = append(cancels, ws_cancel)
		servers = append(servers, ws_server)
	}

	done_sig := make(chan struct{})

	// the server go routine
	// wait on all servers to finish.
	// clean up operation
	// send signal indicating done and close the channel.
	go serverLoop(ctx, done_sig, servers, cancels)

	return done_sig, nil
}

func serverLoop(ctx context.Context, done_sig chan<- struct{}, servers []*http.Server, cancels []context.CancelFunc) {
	var wg sync.WaitGroup

	defer func() {
		wg.Wait()
		done_sig <- struct{}{}
		close(done_sig)
	}()

	for index, server := range servers {
		wg.Add(1)
		go func(index int, server *http.Server, cancel context.CancelFunc) {
			defer wg.Done()
			defer cancel()
			go func() {
				<-ctx.Done()
				Logger.Infof("server shutdown requested: %s", server.Addr)
				// cannot use ctx since that is already closed
				server.Shutdown(context.TODO())
			}()
			select {
			case <-ctx.Done():
			default:
				if err := server.ListenAndServe(); err != http.ErrServerClosed {
					Logger.Errorf("http error %d: %+v", index, err)
				}
			}
		}(index, server, cancels[index])
	}
}
