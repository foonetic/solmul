// solmul is a package for multiplexing solana json rpc api
//
//
// Subscription Workflow
//
// -> user send someSubscribe, with id, which is downstream_method_id
//    `{"jsonrpc":"2.0","method":"accountSubscribe","params":["5oNSm87yBqyKRz2mGqM34xqt2mWKVYvk8CVYKvdnBDBc",{"encoding":"base64","commitment":"processed"}],"id":1}`
//    - create a new mapper_id, mapping to this downstream_id and the downstream_method_id
//    - replace the id with mapper_id, and send it to all upstreams.
//    - set the mapper_id_of_subs[mapper_id] to false, indicating the response is not sent yet.
// <- upstream responds, with id corresponding to the id in the request, and `result` field with a number, which is the subscrption id.
//    `{"jsonrpc":"2.0","result":53,"id":1}`
//    - the mapper_id is the id.
//    - use the mapper_id to find the downstream_id and its downstream_method_id.
//    - if the mapper_id_of_subs[mapper_id] is false, replace the result with downstream_method_id and send the response to downstream_id, and set the mapper_id_of_subs to true.
//    - on this upstream, update sub_id_to_mapper_id, and mapper_id_to_sub_id mapping.
// <- upstream notifies, with subscription id
//    `{"jsonrpc":"2.0","method":"signatureNotification","params":{"result":{"context":{"slot":112513},"value":{"err":null}},"subscription":55}}`
//    - from sub_id_to_mapper_id mapping, find the corresponding mapper_id
//    - from mapper_id, find mapped downstream_id
//    - replace the subscription with the mapper_id, and send it to downstream.
// -> user unsubscribes
//    `{"jsonrpc":"2.0","method":"accountUnsubscribe","params":[64],"id":13}`
//    - the params contains the mapper_id to subscribe.
//    - create a new mapper_id
//    - for each upstream, find the sub_id corresponding to mapper_id.
//    - replace params with [sub_id], and send it to upstream.
//    - send downstream `{"jsonrpc":"2.0","result": true,"id":13}`
// <- unstream responds, with id corresponding to the id, and result be true.
//    `{"jsonrpc":"2.0","result": true,"id":14}`
//    - ignore
//

package solmul

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

func findSubscriptionType(method string) string {
	for _, sub_type := range subscrptionTypes {
		if method == sub_type+"Subscribe" {
			return sub_type
		}
	}

	return ""
}

type indexedPayload struct {
	Payload wsPayload
	Index   int
}

func loopRead(ctx context.Context, conn *websocket.Conn, index int, response_chan chan<- indexedPayload) {
read_loop:
	for {
		_, data, err := conn.ReadMessage()
		// check if the operation is already cancelled
		select {
		case <-ctx.Done():
			break read_loop
		default:
		}

		// error
		if err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure) {
				// continue
			} else if !websocket.IsCloseError(err, websocket.CloseAbnormalClosure, websocket.CloseGoingAway) {
				Logger.Errorf("ws :: reading failed %d: %+v", index, err)
			} else {
				Logger.Debugf("ws :: remote closed: %+v", err)
			}
			break read_loop
		}

		// unmarshall the payload, if error, continue
		value, err := unmarshalWsPayload(data)
		if err != nil {
			Logger.Errorf("ws :: %d failed to unmarshal response: %+v, data: %s", index, err, data)
			continue read_loop
		}

		// send to response
		select {
		case response_chan <- indexedPayload{Payload: value, Index: index}:
		case <-ctx.Done():
			break read_loop
		}
	}
}

func loopWrite(ctx context.Context, conn *websocket.Conn, payload_to_write <-chan wsPayload, index int) {
write_loop:
	for {
		select {
		case <-ctx.Done():
			break write_loop

		case sub_call, ok := <-payload_to_write:
			if !ok {
				break write_loop
			}
			payload, err := sub_call.toJson()
			if err != nil {
				Logger.Warnf("ws :: %d failed to marshal payload %+v", index, err)
				continue write_loop
			}
			if err = conn.WriteMessage(websocket.TextMessage, payload); err != nil {
				Logger.Errorf("ws :: %d failed to send message to remote: %+v; payload: %s", index, err, payload)
			}
		}
	}
}

// Information reguarding an wsUpstream
type wsUpstream struct {
	// Index of the upstream
	Index int
	// Url
	Url string

	// Signals
	RequestChan  chan wsPayload
	ResponseChan chan<- indexedPayload
}

// newUpstream creates a new upstream
func newUpstream(index int, url string, response_chan chan<- indexedPayload) wsUpstream {
	return wsUpstream{
		Index:        index,
		Url:          url,
		ResponseChan: response_chan,
		RequestChan:  make(chan wsPayload),
	}
}

// start starting the upstream.
func (upstream *wsUpstream) start(ctx context.Context) {
	Logger.Debugf("up :: starting upstream %d for %s", upstream.Index, upstream.Url)
	defer Logger.Debugf("up :: shutting down upstream %d for %s", upstream.Index, upstream.Url)
	dialer := websocket.Dialer{}
	dialer_ctx, dialer_cancel := context.WithCancel(ctx)
	defer dialer_cancel()

	conn, response, err := dialer.DialContext(dialer_ctx, upstream.Url, http.Header{})
	if err != nil {
		Logger.Errorf("up :: failed to connect to upstream %d at %s: error: %+v %+v", upstream.Index, upstream.Url, err, response)
		return
	}
	defer conn.Close()

	go loopRead(ctx, conn, upstream.Index, upstream.ResponseChan)

	loopWrite(ctx, conn, upstream.RequestChan, upstream.Index)
	if err = conn.WriteControl(websocket.CloseMessage, []byte{}, time.Now().Add(time.Second)); err != nil {
		Logger.Errorf("up :: failed to write closes msg for %d %s: %+v", upstream.Index, upstream.Url, err)
	}
}

// wsDownstream is the downstream information sent from a downstream to stream mapper
type wsDownstream struct {
	// DownstreamId is the id for the downstream
	DownstreamId int
	// ResponseChan is the channel to send the response to downstream
	ResponseChan chan<- wsPayload
}

// downstreamInfo is the information held by StreamMapper
type downstreamInfo struct {
	// Downstream is the downstream's id and response channel
	*wsDownstream
	// A set of current active subscriptions (by their mapper_id)
	Subscriptions map[uint64]struct{}

	// Cancel cancels the operation to send/receive data by the stream mapper
	Cancel context.CancelFunc
	// Context for the operation
	Context context.Context
}

// send sends a payload to response channel, can be cancelled.
func (downstream *downstreamInfo) send(payloald wsPayload) {
	select {
	case downstream.ResponseChan <- payloald:
	case <-downstream.Context.Done():
		Logger.Debugf("ws :: downstream %d is done", downstream.DownstreamId)
	}
}

type subscriptionInfo struct {
	// Original Method Id
	OriginalMethodId uint64
	// Is the subscription confirmed to the downstream
	IsConfirmed bool
	// type of the subscription
	SubscriptionType string
	// DownstreamId
	DownstreamId int
	// UnsubMapperId
	UnsubscribeMapperId uint64
	// IsUnsubscribed is the mapper id already unsubscribed
	IsUnsubscribed bool
	// Number of upstreams handling this subscription
	Upstreams map[int]struct{}
	// Slot
	Slot uint64
}

type mapperIdList struct {
	List map[uint64]struct{}
}

// Information for mappers
type upstreamMapperInfo struct {
	MapperIdToSubId map[uint64]uint64
	SubIdToMapperId map[uint64]*mapperIdList
}

type streamMapperUpstream struct {
	wsUpstream
	upstreamMapperInfo
	Cancel  context.CancelFunc
	Context context.Context
	// Unsubscribed contains all the ids that have unsubscribed sent
	Unsubscribed map[uint64]struct{}
}

func (upstream *streamMapperUpstream) unsubscribe(mapper_id, unsub_mapper_id uint64, subscription_method string) {
	sub_id, ok := upstream.MapperIdToSubId[mapper_id]
	if !ok {
		if _, ok = upstream.Unsubscribed[mapper_id]; !ok {
			Logger.Errorf("ws :: upstream %d doesn't have subscription for %d", upstream.Index, mapper_id)
		}
		return
	}

	request := wsPayload{
		Unsubscribe: &wsUnsubscribeMethod{
			Id:      unsub_mapper_id,
			JsonRpc: "2.0",
			Method:  subscription_method + "Unsubscribe",
			Params:  []uint64{sub_id},
		},
	}

	delete(upstream.MapperIdToSubId, mapper_id)
	if mapper_list, ok := upstream.SubIdToMapperId[sub_id]; ok {
		delete(mapper_list.List, mapper_id)
		if len(mapper_list.List) > 0 {
			Logger.Infof("ws :: multiple mapper_ids mapped to sub %d: %#v", sub_id, mapper_list.List)
			return
		}

	}

	upstream.Unsubscribed[mapper_id] = struct{}{}

	select {
	case upstream.RequestChan <- request:
		Logger.Debugf("ws :: send unsubscribe to upstream %d: sub id: %+v for mapper_id %d", upstream.Index, request.Unsubscribe.Params, mapper_id)
	case <-upstream.Context.Done():
	}
}

type streamMapper struct {
	Urls     []string
	Upgrader websocket.Upgrader

	// RequestChan is for downstream to send request
	RequestChan chan indexedPayload
	// ResponseChan is for upstream to send response
	ResponseChan chan indexedPayload

	// DownstreamIdChan is the channel to send an unique downstream id to newly connecte downstream.
	DownstreamIdChan chan int
	// AddDownstreamChan is used to add downstream
	AddDownstreamChan chan *wsDownstream
	// DeleteDownstreamChan is used to delete downsteam
	DeleteDownstreamChan chan int
	// Downstreams is the information for downstream
	Downstreams map[int]*downstreamInfo

	CurrentId         uint64
	CurrentResponseId uint64
	// SubscriptionInfo mapper_id to subscription information
	SubscriptionInfo map[uint64]*subscriptionInfo
	// UnsubscribeIds is a set of mapper_ids that are unsubscribe mapper_ids
	UnsubscribeIds map[uint64]struct{}

	// Upstreams
	Upstreams []*streamMapperUpstream
}

func newStreamMapper(urls []string) (*streamMapper, error) {
	if len(urls) == 0 {
		return nil, fmt.Errorf("zero urls")
	}

	return &streamMapper{
		Urls:             urls,
		Upgrader:         websocket.Upgrader{},
		DownstreamIdChan: make(chan int),

		RequestChan:  make(chan indexedPayload),
		ResponseChan: make(chan indexedPayload),

		AddDownstreamChan:    make(chan *wsDownstream),
		DeleteDownstreamChan: make(chan int),
		Downstreams:          make(map[int]*downstreamInfo),

		CurrentId:         1,
		CurrentResponseId: 1,
		SubscriptionInfo:  make(map[uint64]*subscriptionInfo),
		UnsubscribeIds:    make(map[uint64]struct{}),
	}, nil
}

// startDownstreamIdChan launches a go routine and sending downstream ids to startDownstreamIdChan, and downstreams can receive
// from that channel and get the downstream id.
func (stream_mapper *streamMapper) startDownstreamIdChan(ctx context.Context) context.CancelFunc {
	downstream_id_ctx, downstream_id_cancel := context.WithCancel(ctx)
	go func() {
		var downstream_id int = 0
	downstream_id_loop:
		for {
			select {
			case stream_mapper.DownstreamIdChan <- downstream_id:
				downstream_id++
			case <-downstream_id_ctx.Done():
				break downstream_id_loop
			}
		}
		close(stream_mapper.DownstreamIdChan)
	}()
	return downstream_id_cancel
}

func (stream_mapepr *streamMapper) nextId() (id uint64) {
	id = stream_mapepr.CurrentId
	stream_mapepr.CurrentId++
	return
}

// unsubscribe unsubscribe the given mapper_id on all the upstreams
//
// - create an new unsub_mapper_id to send to upstreams.
// - for each upstream, delete the mapper_id from the two maps, and push it onto the unsubed.
// - set the UnsubscribeIds to true
// - delete the upstreams from subscription info
func (stream_mapper *streamMapper) unsubscribe(upstreams []*streamMapperUpstream, mapper_id uint64) {
	info, ok := stream_mapper.SubscriptionInfo[mapper_id]
	if !ok {
		Logger.Errorf("ws :: subscription %d is not valid", mapper_id)
		return
	}

	if info.UnsubscribeMapperId == 0 {
		info.UnsubscribeMapperId = stream_mapper.nextId()
	}

	unsub_mapper_id := info.UnsubscribeMapperId
	info.IsUnsubscribed = true
	stream_mapper.UnsubscribeIds[unsub_mapper_id] = struct{}{}

	for _, upstream := range upstreams {
		_, ok := info.Upstreams[upstream.Index]
		if !ok {
			continue
		}
		delete(info.Upstreams, upstream.Index)
		upstream.unsubscribe(mapper_id, unsub_mapper_id, info.SubscriptionType)
	}
}

func (stream_mapper *streamMapper) initUpstreams(ctx context.Context, upstream_wg *sync.WaitGroup) {
	if len(stream_mapper.Upstreams) == len(stream_mapper.Urls) {
		return
	}
	Logger.Info("ws :: upstream are starting up")
	for index, url := range stream_mapper.Urls {
		upstream_ctx, upstream_cancel := context.WithCancel(ctx)
		upstream := &streamMapperUpstream{
			wsUpstream: newUpstream(index, url, stream_mapper.ResponseChan),
			upstreamMapperInfo: upstreamMapperInfo{
				SubIdToMapperId: make(map[uint64]*mapperIdList),
				MapperIdToSubId: make(map[uint64]uint64),
			},
			Cancel:       upstream_cancel,
			Context:      upstream_ctx,
			Unsubscribed: make(map[uint64]struct{}),
		}
		stream_mapper.Upstreams = append(stream_mapper.Upstreams, upstream)
		upstream_wg.Add(1)
		go func() {
			defer upstream_wg.Done()
			upstream.start(upstream_ctx)
		}()
	}
}

func (stream_mapper *streamMapper) stopUpstreams(upstream_wg *sync.WaitGroup) {
	for mapper_id, sub_info := range stream_mapper.SubscriptionInfo {
		if len(sub_info.Upstreams) > 0 {
			stream_mapper.unsubscribe(stream_mapper.Upstreams, mapper_id)
		}
	}
	for _, upstream := range stream_mapper.Upstreams {
		upstream.Cancel()
	}
	upstream_wg.Wait()
	for _, upstream := range stream_mapper.Upstreams {
		close(upstream.RequestChan)
	}

	stream_mapper.Upstreams = nil
}

// mainLoop starts the mainloop
func (stream_mapper *streamMapper) mainLoop(ctx context.Context) {
	// Launch DownstreamIdChan
	downstream_id_cancel := stream_mapper.startDownstreamIdChan(ctx)
	defer downstream_id_cancel()

	var upstream_wg sync.WaitGroup
	defer stream_mapper.stopUpstreams(&upstream_wg)

	Logger.Info("ws :: launch main loop")

main_loop:
	for {
		select {
		case new_downstream := <-stream_mapper.AddDownstreamChan:
			Logger.Infof("ws :: receiving downstream id: %d", new_downstream.DownstreamId)
			downstream_ctx, downstream_cancel := context.WithCancel(ctx)
			defer downstream_cancel()
			stream_mapper.Downstreams[new_downstream.DownstreamId] = &downstreamInfo{
				wsDownstream:  new_downstream,
				Subscriptions: make(map[uint64]struct{}),
				Context:       downstream_ctx,
				Cancel:        downstream_cancel,
			}
			stream_mapper.initUpstreams(ctx, &upstream_wg)

		case remove_downstream_id := <-stream_mapper.DeleteDownstreamChan:
			stream_mapper.removeDownstream(remove_downstream_id, &upstream_wg)

		case request := <-stream_mapper.RequestChan:
			downstream_id := request.Index
			payload := request.Payload
			if payload.Subscribe != nil {
				stream_mapper.processSubscribeRequest(downstream_id, payload)
			} else if payload.Unsubscribe != nil {
				for _, mapper_id := range payload.Unsubscribe.Params {
					stream_mapper.unsubscribe(stream_mapper.Upstreams, mapper_id)
				}
				if downstream, ok := stream_mapper.Downstreams[downstream_id]; ok {
					downstream.send(wsPayload{ConfirmUnsubscribe: &wsConfirmUnsubscribe{
						JsonRpc: payload.Unsubscribe.JsonRpc,
						Id:      payload.Unsubscribe.Id,
						Result:  true,
					}})
				} else {
					Logger.Errorf("ws :: unsubscribe is from an unknown downstream %d", downstream_id)
				}
			} else if payload.Ping != nil { // ignore ping
			} else {
				Logger.Errorf("ws :: unknown payload from downstream %d: %#v", downstream_id, payload)
			}

		case response := <-stream_mapper.ResponseChan:
			upstream_index, payload := response.Index, response.Payload
			if payload.ConfirmSubscribe != nil {
				stream_mapper.processConfirmSubscribe(payload, stream_mapper.Upstreams[upstream_index])
			} else if payload.ConfirmUnsubscribe != nil { // ignore ConfirmUnsubscribe because it's alreayd confirmed
			} else if payload.Notification != nil {
				stream_mapper.processNotification(payload, stream_mapper.Upstreams[upstream_index])
			} else if payload.Error != nil {
				Logger.Warnf("ws :: error from upstream %d: %s", upstream_index, payload.OriginalData)
			} else {
				Logger.Errorf("ws :: unknown payload from upstream %d: %s", upstream_index, payload.OriginalData)
			}

		case <-ctx.Done():
			Logger.Debugf("ws :: cancelled main loop")
			break main_loop
		}
	}
}

func (stream_mapper *streamMapper) removeDownstream(remove_downstream_id int, upstream_wg *sync.WaitGroup) {
	Logger.Infof("ws :: removing downstream id: %d", remove_downstream_id)
	downstream_info, is_downstream := stream_mapper.Downstreams[remove_downstream_id]
	if !is_downstream {
		Logger.Errorf("ws :: cannot find downstream id to remove: %d", remove_downstream_id)
		return
	}
	for mapper_id := range downstream_info.Subscriptions {
		stream_mapper.unsubscribe(stream_mapper.Upstreams, mapper_id)
	}
	downstream_info.Cancel()
	close(downstream_info.ResponseChan)
	delete(stream_mapper.Downstreams, remove_downstream_id)

	if len(stream_mapper.Downstreams) == 0 {
		stream_mapper.stopUpstreams(upstream_wg)
	}
}

func (stream_mapper *streamMapper) processConfirmSubscribe(payload wsPayload, upstream *streamMapperUpstream) {
	mapper_id, sub_id := payload.ConfirmSubscribe.Id, payload.ConfirmSubscribe.Result

	Logger.Debugf("ws :: upstream %d confirms mapper_id %d with subscription id %d", upstream.Index, mapper_id, sub_id)

	info, ok := stream_mapper.SubscriptionInfo[mapper_id]
	if !ok {
		Logger.Errorf("ws :: cannot find mapper id %d", mapper_id)
		return
	}
	send_confirm := !info.IsConfirmed
	info.IsConfirmed = true
	info.Upstreams[upstream.Index] = struct{}{}
	if v, ok := upstream.SubIdToMapperId[sub_id]; ok {
		v.List[mapper_id] = struct{}{}
	} else {
		upstream.SubIdToMapperId[sub_id] = &mapperIdList{List: map[uint64]struct{}{mapper_id: {}}}
	}
	upstream.MapperIdToSubId[mapper_id] = sub_id
	downstream, ok := stream_mapper.Downstreams[info.DownstreamId]
	if !ok {
		Logger.Errorf("ws :: cannot find downstream %d for mapper_id %d", info.DownstreamId, mapper_id)
		return
	}

	if send_confirm {
		payload.ConfirmSubscribe.Id = info.OriginalMethodId
		payload.ConfirmSubscribe.Result = mapper_id
		downstream.send(payload)
	}
}

func (stream_mapper *streamMapper) processNotification(payload wsPayload, upstream *streamMapperUpstream) {
	notification := payload.Notification

	if notification.Params == nil {
		Logger.Errorf("ws :: notification has no params: %#v", *notification)
		return
	}

	subscription_id := notification.Params.Subscription
	mapper_id_list, ok := upstream.SubIdToMapperId[subscription_id]
	if !ok {
		Logger.Errorf("ws :: upstream %d doesn't have subscription_id %d", upstream.Index, subscription_id)
		return
	}
	response_id := stream_mapper.CurrentResponseId
	stream_mapper.CurrentResponseId++

	if len(mapper_id_list.List) == 0 {
		Logger.Warnf("ws :: zero mapper for subscription %d", subscription_id)
	}

	slot := notification.getSlot()
	for mapper_id := range mapper_id_list.List {
		info, ok := stream_mapper.SubscriptionInfo[mapper_id]
		if !ok {
			Logger.Errorf("ws :: mapper_id %d is not a subscription", mapper_id)
			continue
		}
		if info.IsUnsubscribed {
			continue
		}

		// signature notification will be automatically cancelled
		if notification.Method == "signatureNotification" {
			upstream.Unsubscribed[mapper_id] = struct{}{}
			info.IsUnsubscribed = true
			delete(mapper_id_list.List, mapper_id)
			delete(upstream.MapperIdToSubId, mapper_id)
		}

		if slot <= info.Slot {
			continue
		}
		info.Slot = slot

		downstream, ok := stream_mapper.Downstreams[info.DownstreamId]
		if !ok {
			Logger.Errorf("ws :: mapper_id %d's downstream %d doesn't exist", mapper_id, info.DownstreamId)
			continue
		}
		notification.Params.Subscription = mapper_id
		notification.Id = response_id
		downstream.send(payload)
	}
}

func (stream_mapper *streamMapper) processSubscribeRequest(downstream_id int, payload wsPayload) {
	upstreams := stream_mapper.Upstreams
	downstream, ok := stream_mapper.Downstreams[downstream_id]
	if !ok {
		Logger.Errorf("ws :: cannot find downstream id %d", downstream_id)
		return
	}
	subscribe_method := payload.Subscribe
	mapper_id := stream_mapper.nextId()
	info := &subscriptionInfo{
		DownstreamId:     downstream_id,
		IsConfirmed:      false,
		IsUnsubscribed:   false,
		OriginalMethodId: subscribe_method.Id,
		SubscriptionType: findSubscriptionType(subscribe_method.Method),
		Upstreams:        make(map[int]struct{}),
		Slot:             0,
	}
	stream_mapper.SubscriptionInfo[mapper_id] = info
	downstream.Subscriptions[mapper_id] = struct{}{}
	payload.Subscribe.Id = mapper_id
	Logger.Debugf("ws :: mapping downstream %d method id %d to mapper_id %d", downstream_id, info.OriginalMethodId, mapper_id)
	for _, upstream := range upstreams {
		select {
		case upstream.RequestChan <- payload:
		case <-upstream.Context.Done():
		}
	}
}

func (stream_mapper *streamMapper) runDownstream(w http.ResponseWriter, req *http.Request) {
	conn, err := stream_mapper.Upgrader.Upgrade(w, req, nil)
	if err != nil {
		Logger.Errorf("ws :: failed to upgrade websocket connection: %+v", err)
		return
	}
	defer func() {
		conn.WriteControl(websocket.CloseMessage, []byte{}, <-time.After(time.Second))
		conn.Close()
	}()

	response_chan := make(chan wsPayload)
	downstream_id := <-stream_mapper.DownstreamIdChan
	new_downstream := &wsDownstream{
		DownstreamId: downstream_id,
		ResponseChan: response_chan,
	}

	select {
	case stream_mapper.AddDownstreamChan <- new_downstream:
		Logger.Infof("ws :: downstream id: %d created", downstream_id)
		defer func() {
			stream_mapper.DeleteDownstreamChan <- downstream_id
		}()
	case <-req.Context().Done():
		Logger.Debugf("ws :: downstream cancelled %d", downstream_id)
		return
	}

	ctx, cancel := context.WithCancel(req.Context())
	defer cancel()

	go loopWrite(ctx, conn, response_chan, downstream_id)

	loopRead(ctx, conn, downstream_id, stream_mapper.RequestChan)
}
