package solmul

import (
	"testing"
)

func TestUnmarshalWsPayload(t *testing.T) {
	to_test := map[string]string{
		`subscribe`:           `{"jsonrpc":"2.0","method":"accountSubscribe","params":["5oNSm87yBqyKRz2mGqM34xqt2mWKVYvk8CVYKvdnBDBc",{"encoding":"base64","commitment":"processed"}],"id":1}`,
		`confirm_subscribe`:   `{"jsonrpc":"2.0","result":53,"id":1}`,
		`notification`:        `{"jsonrpc":"2.0","method":"signatureNotification","params":{"result":{"context":{"slot":112513},"value":{"err":null}},"subscription":55}}`,
		`unsubscribe`:         `{"jsonrpc":"2.0","method":"accountUnsubscribe","params":[64],"id":13}`,
		`confirm_unsubscribe`: `{"jsonrpc":"2.0","result": true,"id":14}`,
	}

	for desired_type, payload_to_parse := range to_test {
		result, err := UnmarshalWsPayload([]byte(payload_to_parse))
		if err != nil {
			t.Errorf("failed to parse the payload: %+v: %s", err, payload_to_parse)
		} else {
			is_good := false
			switch desired_type {
			case "subscribe":
				is_good = result.Subscribe != nil
			case "unsubscribe":
				is_good = result.Unsubscribe != nil
			case "notification":
				is_good = result.Notification != nil
			case "confirm_subscribe":
				is_good = result.ConfirmSubscribe != nil
			case "confirm_unsubscribe":
				is_good = result.ConfirmUnsubscribe != nil
			}

			if !is_good {
				t.Errorf("expecting %s type, but data is %+v", desired_type, result)
			}
		}
	}
}
