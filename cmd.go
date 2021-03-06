// solmul is a package for multiplexing solana json rpc api
//
// - for all rpc calls except send transaction, the first response is sent back.
//
// - for all subscription calls, the notification is inspected by looking at the slot number - and only the notification with slot greater than the last seen slot number is sent back.
//
// After installation, run the command like below
//
//     solmul -u mainnet-beta -u mainnet-beta-serum
//
package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/foonetic/solmul/solmul"
	"github.com/spf13/cobra"
)

var (
	urls              []string
	rpc_port, ws_port int
)

func mainFunc(cmd *cobra.Command, args []string) {
	var rpc_urls, ws_urls []string
	for _, a_url := range urls {
		if endpoint, err := solmul.ParseEndpoint(a_url); err == nil {
			rpc_urls = append(rpc_urls, endpoint.Rpc)
			ws_urls = append(ws_urls, endpoint.Websocket)
		} else {
			solmul.Logger.Errorf("%v is not a valid url\n", a_url)
		}
	}

	// set up cancel
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	waiter, err := solmul.RunServer(ctx, rpc_urls, ws_urls, rpc_port, ws_port)
	if err != nil {
		solmul.Logger.Fatalf("failed to start server: %+v", err)
	}

	// set up signal handling for Ctrl+C/SIGINT
	sig_chan := make(chan os.Signal, 6)
	signal.Notify(sig_chan, syscall.SIGINT)

	var wait_for_server_finish sync.WaitGroup
	wait_for_server_finish.Add(1)
	go func() {
		// clean out the wait group.
		defer wait_for_server_finish.Done()
		sigint_called := 0
		// waiter loop.
		// this needs to be a loop since multiple SIGINTs can be sent before the process exits.
	waiter_loop:
		for {
			select {
			case <-sig_chan:
				sigint_called++
				solmul.Logger.Infof("SIGINT received: %d, shutdown", sigint_called)
				cancel()
				// if more than 5 sigints have been received, exit right away.
				if sigint_called >= 5 {
					os.Exit(1)
				}
			case <-waiter:
				break waiter_loop
			}
		}
	}()

	wait_for_server_finish.Wait()
}

func main() {
	rootCmd := &cobra.Command{
		Use:   "solmul",
		Short: "Solana RPC Multiplexer",
		Long: `Solana RPC Multiplexer.

Sending the same RPC/WebSocket requests to multiple solana validators.

- for all rpc calls except send transaction, the first response is sent back.

- for all subscription calls, the notification is inspected by looking at the slot number - and
  only the notification with slot greater than the last seen slot number is sent back.
`,
		Run: mainFunc,
	}

	rootCmd.Flags().StringArrayVarP(&urls, "url", "u", []string{}, `URLs for Solana RPC.
Can be a URL like http://127.0.0.1:8899 or mainnet-beta|devnet|testnet|mainnet-beta-serum.
Specify multiple times for mulitple validators, such as "-u url1 -u url2 ..."`)
	rootCmd.MarkFlagRequired("url")
	rootCmd.Flags().IntVar(&rpc_port, "rpc-port", 8899, "rpc port for the service")
	rootCmd.Flags().IntVar(&ws_port, "ws-port", 8900, "websocket port for the service")

	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}
