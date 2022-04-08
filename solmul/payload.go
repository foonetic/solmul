package solmul

import (
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
)

// arrayContains check if a value is in an array.
func arrayContains[T comparable](array []T, value T) bool {
	for _, v := range array {
		if v == value {
			return true
		}
	}
	return false
}

func toJson[T any](v T) ([]byte, error) {
	return bson.MarshalExtJSON(v, false, false)
}

// all possible subscription types.
var subscrptionTypes = [...]string{
	"account",
	"block",
	"logs",
	"program",
	"signature",
	"slot",
	"slotsUpdates",
	"root",
	"vote",
}

var (
	// allSubscribeMethods all subscribe methods
	allSubscribeMethods []string
	// allUnsubscribeMethods all unsubscribe methods
	allUnsubscribeMethods []string
	// allNotificationMethods all notification methods
	allNotificationMethods []string
)

func init() {
	for _, subscriptionType := range subscrptionTypes {
		allNotificationMethods = append(allNotificationMethods, subscriptionType+"Notification")
		allSubscribeMethods = append(allSubscribeMethods, subscriptionType+"Subscribe")
		allUnsubscribeMethods = append(allUnsubscribeMethods, subscriptionType+"Unsubscribe")
	}
}

// UnknownJsonMap contains all the value that is in json but not explicitly defined in struct
type UnknownJsonMap map[string]bson.RawValue

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// WebSocket

// wsConfirmSubscribe is the confirmation that server has set up the subscription
// and the subscription id used to cancel the request in is in the result.
// `{"jsonrpc":"2.0","result":53,"id":1}`
type wsConfirmSubscribe struct {
	Id      uint64 `bson:"id"`
	JsonRpc string `bson:"jsonrpc"`
	Result  uint64 `bson:"result"`
}

// wsConfirmUnsubscribe is the confirmation that server has stopped the subscription.
type wsConfirmUnsubscribe struct {
	Id      uint64 `bson:"id"`
	JsonRpc string `bson:"jsonrpc"`
	Result  bool   `bson:"result"`
}

type wsError struct {
	Id      uint64         `bson:"id"`
	JsonRpc string         `bson:"jsonrpc"`
	Error   *bson.RawValue `bson:"error,omitempty"`

	UnknownJsonMap `bson:",inline"`
}

// wsSubscribeMethod
type wsSubscribeMethod struct {
	Id      uint64         `bson:"id"`
	JsonRpc string         `bson:"jsonrpc"`
	Method  string         `bson:"method"`
	Params  *bson.RawValue `bson:"params,omitempty"`

	UnknownJsonMap `bson:",inline"`
}

// wsUnsubscribeMethod
type wsUnsubscribeMethod struct {
	Id      uint64   `bson:"id"`
	JsonRpc string   `bson:"jsonrpc"`
	Method  string   `bson:"method"`
	Params  []uint64 `bson:"params"`

	UnknownJsonMap `bson:",inline"`
}

// wsNotificationParams is the body of the notification
type wsNotificationParams struct {
	UnknownJsonMap `bson:",inline"`
	Result         *struct {
		Context *struct {
			UnknownJsonMap `bson:",inline"`
			// Slot is the slot number this notification is for
			Slot uint64 `bson:"slot"`
		} `bson:"context,omitempty"`
		UnknownJsonMap `bson:",inline"`
	} `bson:"result,omitempty"`
	// Subscription contains the subscription id
	Subscription uint64 `bson:"subscription"`
}

type wsNotificationMethod struct {
	Id      uint64                `bson:"id"`
	JsonRpc string                `bson:"jsonrpc"`
	Method  string                `bson:"method"`
	Params  *wsNotificationParams `bson:"params,omitempty"`
}

func (params *wsNotificationMethod) getSlot() uint64 {
	if params.Params == nil {
		Logger.Error("ws :: notification has no parameter")
		return 0
	}
	// followings are with context
	// "accountNotification"  "logNotification"  "programNotification"  "signatureNotification" {
	if params.Params.Result != nil && params.Params.Result.Context != nil {
		return params.Params.Result.Context.Slot
	}
	// logrus.Errorf("%s: %#v", params.Method, *params.Params)
	if params.Method == "slotNotification" {
		if v, is_in := params.Params.Result.UnknownJsonMap["slot"]; is_in {
			result, ok := v.AsInt64OK()
			if ok {
				return uint64(result)
			}
		} else {
			Logger.Error("ws :: slot notification has no result")
		}
	} else if params.Method == "rootNotification" {
		if v, is_in := params.Params.UnknownJsonMap["result"]; is_in {
			if r, ok := v.AsInt64OK(); ok {
				return uint64(r)
			}
		}
	}

	return 0
}

// wsPing is the ping message
type wsPing struct {
	JsonRpc string `bson:"jsonrpc"`
	Method  string `bson:"method,omitempty"`
	// params in ping should be null
	Params interface{} `bson:"params"`
}

type wsPayload struct {
	Subscribe          *wsSubscribeMethod
	Unsubscribe        *wsUnsubscribeMethod
	Notification       *wsNotificationMethod
	ConfirmSubscribe   *wsConfirmSubscribe
	ConfirmUnsubscribe *wsConfirmUnsubscribe
	Error              *wsError
	Ping               *wsPing

	OriginalData []byte
}

type wsPossible struct {
	Id      uint64         `bson:"id"`
	JsonRpc string         `bson:"jsonrpc"`
	Method  string         `bson:"method,omitempty"`
	Params  *bson.RawValue `bson:"params,omitempty"`
	Result  *bson.RawValue `bson:"result,omitempty"`
	Error   *bson.RawValue `bson:"error,omitempty"`
}

func (payload wsPayload) toJson() ([]byte, error) {
	if payload.ConfirmSubscribe != nil {
		return toJson(*payload.ConfirmSubscribe)
	}
	if payload.ConfirmUnsubscribe != nil {
		return toJson(*payload.ConfirmUnsubscribe)
	}
	if payload.Notification != nil {
		return toJson(*payload.Notification)
	}
	if payload.Subscribe != nil {
		return toJson(*payload.Subscribe)
	}
	if payload.Unsubscribe != nil {
		return toJson(*payload.Unsubscribe)
	}
	if payload.Ping != nil {
		return toJson(*payload.Ping)
	}
	if payload.Error != nil {
		return toJson(*payload.Error)
	}

	return nil, fmt.Errorf("no data in payload")
}

// unmarshalWsPayload gets a WsPayload from data.
func unmarshalWsPayload(data []byte) (wsPayload, error) {
	var possbile_value wsPossible
	err := bson.UnmarshalExtJSON(data, false, &possbile_value)
	if err != nil {
		return wsPayload{}, fmt.Errorf("failed to unmarshal json payload: %+v", err)
	}

	result := wsPayload{}

	result.OriginalData = data

	if possbile_value.Error != nil {
		result.Error = &wsError{}
		err = bson.UnmarshalExtJSON(data, false, result.Error)
	} else if possbile_value.Result != nil {
		if possbile_value.Result.Type == bson.TypeBoolean {
			result.ConfirmUnsubscribe = &wsConfirmUnsubscribe{}
			err = bson.UnmarshalExtJSON(data, false, result.ConfirmUnsubscribe)
		} else {
			result.ConfirmSubscribe = &wsConfirmSubscribe{}
			err = bson.UnmarshalExtJSON(data, false, result.ConfirmSubscribe)
		}
	} else if arrayContains(allSubscribeMethods, possbile_value.Method) {
		result.Subscribe = &wsSubscribeMethod{}
		err = bson.UnmarshalExtJSON(data, false, result.Subscribe)
	} else if arrayContains(allUnsubscribeMethods, possbile_value.Method) {
		result.Unsubscribe = &wsUnsubscribeMethod{}
		err = bson.UnmarshalExtJSON(data, false, result.Unsubscribe)
	} else if arrayContains(allNotificationMethods, possbile_value.Method) {
		result.Notification = &wsNotificationMethod{}
		err = bson.UnmarshalExtJSON(data, false, result.Notification)
	} else if possbile_value.Method == "ping" {
		result.Ping = &wsPing{}
		err = bson.UnmarshalExtJSON(data, false, result.Ping)
	} else {
		err = fmt.Errorf("unknown payoad: %s", data)
	}

	if err != nil {
		err = fmt.Errorf("failed to get payload: %+v", err)
		return wsPayload{}, err
	}

	return result, nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Rpc

// sendTransactionMethod is the method to send a transaction.
const sendTransactionMethod = "sendTransaction"

type rpcMethodCommon struct {
	Id      string `bson:"id"`
	JsonRpc string `bson:"jsonrpc"`
}

type rpcMethodCall struct {
	rpcMethodCommon `bson:",inline"`
	UnknownJsonMap  `bson:",inline"`
	Method          string `bson:"method"`
}
