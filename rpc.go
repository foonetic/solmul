package solmul

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"

	"go.mongodb.org/mongo-driver/bson"
)

// rpcCaller contains the all the upstream urls to forward requests to.
type rpcCaller struct {
	Urls []string
}

type RpcResponseWithCode struct {
	Body       []byte
	StatusCode int
}

// a handler with multicaller.
func (multi_caller *rpcCaller) HandleRequest(w http.ResponseWriter, req *http.Request) {
	req_body, err := io.ReadAll(req.Body)
	defer req.Body.Close()
	if err != nil {
		return
	}

	var method_call RpcMethodCall
	err = bson.UnmarshalExtJSON(req_body, false, &method_call)
	if err != nil {
		Logger.Errorf("rpc :: failed to parse requestion body: %+v %s", err, req_body)
		return
	}

	ctx, cancel := context.WithCancel(req.Context())

	resp_chan := make(chan *RpcResponseWithCode)

	for index, url := range multi_caller.Urls {
		// only send transaction to the first upstream.
		if method_call.Method == SendTransactionMethod && index > 0 {
			continue
		}

		go func(an_url string, index int) {
			// forwarding the request
			request_to_upstream, err := http.NewRequestWithContext(ctx, http.MethodPost, an_url, bytes.NewReader(req_body))
			request_to_upstream.Header.Add("content-type", "application/json")
			response, err := http.DefaultClient.Do(request_to_upstream)
			if errors.Is(err, context.Canceled) {
				return
			}
			if err != nil {
				Logger.Errorf("rpc :: error at %d: %v", index, err)
				return
			}
			// response status code indicates an error.
			if response.StatusCode >= 400 {
				Logger.Errorf("rpc :: error response from upstream %d: %v", index, response.Status)
				return
			}

			res_body, err := io.ReadAll(response.Body)
			defer response.Body.Close()

			if err != nil {
				Logger.Errorf("rpc :: failed to read response from upstream %d: %+v", index, err)
			}

			select {
			// send the response back
			case resp_chan <- &RpcResponseWithCode{
				Body:       res_body,
				StatusCode: response.StatusCode,
			}:
			default:
				return
			}
		}(url, index)
	}

	var resp *RpcResponseWithCode

	select {
	case resp = <-resp_chan:
		cancel()
	case <-req.Context().Done():
		Logger.Debugf("rpc :: downstream cancelled")
		w.WriteHeader(http.StatusInternalServerError)
		cancel()
		return
	}

	w.WriteHeader(resp.StatusCode)
	w.Header().Add("content-type", "application/json")
	w.Write(resp.Body)
}
