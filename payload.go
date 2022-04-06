package solmul

import (
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
)

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

// UnknownJsonMap is contains all the value that is in the struct explicitly body.
type UnknownJsonMap map[string]bson.RawValue

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// WebSocket

type WsMethodCommon struct {
	Id      uint64 `bson:"id"`
	JsonRpc string `bson:"jsonrpc"`
}

func NewWsMethodCommon(id uint64) WsMethodCommon {
	return WsMethodCommon{Id: id, JsonRpc: "2.0"}
}

// WsConfirmSubscribe is the confirmation that server has set up the subscription
// and the subscription id used to cancel the request in is in the result.
// `{"jsonrpc":"2.0","result":53,"id":1}`
type WsConfirmSubscribe struct {
	WsMethodCommon `bson:",inline"`
	Result         uint64 `bson:"result"`
}

// WsConfirmUnsubscribe is the confirmation that server has stopped the subscription.
type WsConfirmUnsubscribe struct {
	WsMethodCommon `bson:",inline"`
	Result         bool `bson:"result"`
}

type WsError struct {
	WsMethodCommon `bson:",inline"`
	Error          *bson.RawValue `bson:"error,omitempty"`

	UnknownJsonMap `bson:",inline"`
}

// WsSubscribeMethod
type WsSubscribeMethod struct {
	WsMethodCommon `bson:",inline"`
	Method         string         `bson:"method"`
	Params         *bson.RawValue `bson:"params,omitempty"`

	UnknownJsonMap `bson:",inline"`
}

// WsUnsubscribeMethod
type WsUnsubscribeMethod struct {
	WsMethodCommon `bson:",inline"`
	Method         string   `bson:"method"`
	Params         []uint64 `bson:"params"`

	UnknownJsonMap `bson:",inline"`
}

// WsNotificationParams is the body of the notification
type WsNotificationParams struct {
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

type WsNotificationMethod struct {
	WsMethodCommon `bson:",inline"`
	Method         string                `bson:"method"`
	Params         *WsNotificationParams `bson:"params,omitempty"`
}

type WsSlotNotificationParamsResult struct {
	Parent uint64 `bson:"parent"`
	Root   uint64 `bson:"root"`
	Slot   uint64 `bson:"slot"`
}

func (params *WsNotificationMethod) GetSlot() uint64 {
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

// WsPing is the ping message
type WsPing struct {
	JsonRpc string `bson:"jsonrpc"`
	Method  string `bson:"method,omitempty"`
	// params in ping should be null
	Params interface{} `bson:"params"`
}

type WsPayload struct {
	Subscribe          *WsSubscribeMethod
	Unsubscribe        *WsUnsubscribeMethod
	Notification       *WsNotificationMethod
	ConfirmSubscribe   *WsConfirmSubscribe
	ConfirmUnsubscribe *WsConfirmUnsubscribe
	Error              *WsError
	Ping               *WsPing

	OriginalData []byte
}

type WsPossible struct {
	WsMethodCommon `bson:",inline"`
	Method         string         `bson:"method,omitempty"`
	Params         *bson.RawValue `bson:"params,omitempty"`
	Result         *bson.RawValue `bson:"result,omitempty"`
	Error          *bson.RawValue `bson:"error,omitempty"`
}

func (payload WsPayload) ToJson() ([]byte, error) {
	if payload.ConfirmSubscribe != nil {
		return ToJson(*payload.ConfirmSubscribe)
	}
	if payload.ConfirmUnsubscribe != nil {
		return ToJson(*payload.ConfirmUnsubscribe)
	}
	if payload.Notification != nil {
		return ToJson(*payload.Notification)
	}
	if payload.Subscribe != nil {
		return ToJson(*payload.Subscribe)
	}
	if payload.Unsubscribe != nil {
		return ToJson(*payload.Unsubscribe)
	}
	if payload.Ping != nil {
		return ToJson(*payload.Ping)
	}
	if payload.Error != nil {
		return ToJson(*payload.Error)
	}

	return nil, fmt.Errorf("no data in payload")
}

// UnmarshalWsPayload gets a WsPayload from data.
func UnmarshalWsPayload(data []byte) (WsPayload, error) {
	var possbile_value WsPossible
	err := bson.UnmarshalExtJSON(data, false, &possbile_value)
	if err != nil {
		return WsPayload{}, fmt.Errorf("failed to unmarshal json payload: %+v", err)
	}

	result := WsPayload{}

	result.OriginalData = data

	if possbile_value.Error != nil {
		result.Error = &WsError{}
		err = bson.UnmarshalExtJSON(data, false, result.Error)
	} else if possbile_value.Result != nil {
		if possbile_value.Result.Type == bson.TypeBoolean {
			result.ConfirmUnsubscribe = &WsConfirmUnsubscribe{}
			err = bson.UnmarshalExtJSON(data, false, result.ConfirmUnsubscribe)
		} else {
			result.ConfirmSubscribe = &WsConfirmSubscribe{}
			err = bson.UnmarshalExtJSON(data, false, result.ConfirmSubscribe)
		}
	} else if ArrayContains(allSubscribeMethods, possbile_value.Method) {
		result.Subscribe = &WsSubscribeMethod{}
		err = bson.UnmarshalExtJSON(data, false, result.Subscribe)
	} else if ArrayContains(allUnsubscribeMethods, possbile_value.Method) {
		result.Unsubscribe = &WsUnsubscribeMethod{}
		err = bson.UnmarshalExtJSON(data, false, result.Unsubscribe)
	} else if ArrayContains(allNotificationMethods, possbile_value.Method) {
		result.Notification = &WsNotificationMethod{}
		err = bson.UnmarshalExtJSON(data, false, result.Notification)
	} else if possbile_value.Method == "ping" {
		result.Ping = &WsPing{}
		err = bson.UnmarshalExtJSON(data, false, result.Ping)
	} else {
		err = fmt.Errorf("unknown payoad: %s", data)
	}

	if err != nil {
		err = fmt.Errorf("failed to get payload: %+v", err)
		return WsPayload{}, err
	}

	return result, nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Rpc

// SendTransactionMethod is the method to send a transaction.
const SendTransactionMethod = "sendTransaction"

type RpcMethodCommon struct {
	Id      string `bson:"id"`
	JsonRpc string `bson:"jsonrpc"`
}

type RpcMethodCall struct {
	RpcMethodCommon `bson:",inline"`
	UnknownJsonMap  `bson:",inline"`
	Method          string `bson:"method"`
}

type RpcMethodResultObject struct {
	UnknownJsonMap `bson:",inline"`
	Context        struct {
		UnknownJsonMap `bson:",inline"`
		Slot           uint64 `bson:"slot"`
	} `bson:"context"`
}

type RpcMethodResult struct {
	RpcMethodCommon `bson:",inline"`
	UnknownJsonMap  `bson:",inline"`
	Result          bson.RawValue `bson:"result"`
}
