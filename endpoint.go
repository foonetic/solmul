package solmul

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
)

type Endpoint struct {
	Rpc       string
	Websocket string
}

func GetEndpoint(input string) (endpoint Endpoint, err error) {
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

	endpoint = Endpoint{
		Rpc:       realUrl,
		Websocket: ws_schema + "://" + host + ws_port,
	}

	return
}
