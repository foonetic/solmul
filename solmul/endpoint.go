package solmul

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
)

// endPoint contains the parsed rpc endpoint and websocket endpoint
type endPoint struct {
	Rpc       string
	Websocket string
}

// ParseEndpoint translates a string into proper solana endpoint.
//
// • mainnet-beta, localhost, devnet, testnet, and mainnet-beta-serum is translated into proper urls.
//
// • other strings are treated like urls.
//
// • web socket port will be rpc port number if rpc port number is explicitly set; otherwise it assumes
// web socket and rpc are running on same port.
//
func ParseEndpoint(input string) (endpoint endPoint, err error) {
	realUrl := input
	switch input {
	case "devnet":
		realUrl = "https://api.devnet.solana.com"
	case "mainnet-beta":
		realUrl = "https://api.mainnet-beta.solana.com"
	case "testnet":
		realUrl = "https://api.testnet.solana.com"
	case "localhost":
		realUrl = "http://127.0.0.1:8899"
	case "mainnet-beta-serum":
		realUrl = "https://solana-api.projectserum.com"
	default:
		realUrl = input
	}

	parsed, err := url.Parse(realUrl)
	if err != nil {
		return
	}

	if parsed.Scheme == "" {
		err = fmt.Errorf("url doesn't contain protocol \"%s\"", realUrl)
		return
	}

	host, port, err := net.SplitHostPort(parsed.Host)
	if err != nil {
		err = nil
		host = parsed.Host
	}

	ws_port := ""
	if port != "" {
		port_number, err := strconv.Atoi(port)
		if err != nil {
			return endpoint, err
		}
		ws_port = ":" + strconv.Itoa(port_number+1)
	}

	ws_schema := "ws"
	if parsed.Scheme == "https" {
		ws_schema = "wss"
	}

	endpoint = endPoint{
		Rpc:       realUrl,
		Websocket: ws_schema + "://" + host + ws_port,
	}

	return
}
