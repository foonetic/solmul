import express from "express";
import WebSocket, { WebSocketServer } from "ws";
import fetch from "cross-fetch";
import { ArgumentParser } from "argparse";
import { URL } from "url";

/////////////////////////////////////////////////////////////////////////////
// get url

/**
 * get the solana endpoint url
 *
 * @param input input string
 * @returns a value with rpc_url and ws_url
 */
function getUrl(input: string) {
  let real_url: string;

  switch (input) {
    case "devnet":
      real_url = "https://api.devnet.solana.com";
      break;
    case "mainnet-beta":
      real_url = "https://api.mainnet-beta.solana.com";
      break;
    case "testnet":
      real_url = "https://api.testnet.solana.com";
      break;
    case "mainnet-beta-serum":
      real_url = "https://solana-api.projectserum.com";
      break;
    default:
      real_url = input;
      break;
  }
  const parsed_url = new URL(real_url);
  const ws_port = parsed_url.port === "" ? "" : `:${parseInt(parsed_url.port) + 1}`;
  const ws_protocol = parsed_url.protocol === "https:" ? "wss:" : "ws:";
  return {
    rpc_url: `${parsed_url.protocol}//${parsed_url.host}`,
    ws_url: `${ws_protocol}//${parsed_url.hostname}${ws_port}`,
  };
}

/////////////////////////////////////////////////////////////////////////////
// web socket

/**
 * method call always have the id (which potentially is a number, and the method name)
 */
interface WebSocketMethodCall {
  id: number;
  method: string;
}

/**
 * Subscribe method.
 */
interface WebSocketSubcribe extends WebSocketMethodCall {
  params: any;
}

/**
 * Unsubscribe method.
 */
interface WebSocketUnsubscribe extends WebSocketMethodCall {
  /**
   * params of unsubscribe method, which contains one single number that is the subscription id.
   */
  params: number[];
}

/**
 * notification from server to client.
 */
interface WebSocketNotification extends WebSocketMethodCall {
  /**
   * besides all the other payload information, subscription is the subscription id that this notification is about.
   */
  params: {
    subscription: number;
  };
}

/**
 * Response to a subscribe/unsubscribe request.
 */
interface WebSocketResponse {
  id: number;
  /**
   * result for the response.
   * - if this is a response to subscribe, the value will be the subscripton id, which is a number
   * - if this is a response to unsubscribe, the value will be `true`
   */
  result: number | boolean;
}

/**
 * Subscription Workflow
 *
 * -> user send someSubscribe, with id, which is downstream_method_id
 *    `{"jsonrpc":"2.0","method":"accountSubscribe","params":["5oNSm87yBqyKRz2mGqM34xqt2mWKVYvk8CVYKvdnBDBc",{"encoding":"base64","commitment":"processed"}],"id":1}`
 *    - create a new mapper_id, mapping to this downstream_id and the downstream_method_id
 *    - replace the id with mapper_id, and send it to all upstreams.
 *    - set the mapper_id_of_subs[mapper_id] to false, indicating the response is not sent yet.
 * <- upstream responds, with id corresponding to the id in the request, and `result` field with a number, which is the subscrption id.
 *    `{"jsonrpc":"2.0","result":53,"id":1}`
 *    - the mapper_id is the id.
 *    - use the mapper_id to find the downstream_id and its downstream_method_id.
 *    - if the mapper_id_of_subs[mapper_id] is false, replace the result with downstream_method_id and send the response to downstream_id, and set the mapper_id_of_subs to true.
 *    - on this upstream, update sub_id_to_mapper_id, and mapper_id_to_sub_id mapping.
 * <- upstream notifies, with subscription id
 *    `{"jsonrpc":"2.0","method":"signatureNotification","params":{"result":{"context":{"slot":112513},"value":{"err":null}},"subscription":55}}`
 *    - from sub_id_to_mapper_id mapping, find the corresponding mapper_id
 *    - from mapper_id, find mapped downstream_id
 *    - replace the subscription with the mapper_id, and send it to downstream.
 * -> user unsubscribes
 *    `{"jsonrpc":"2.0","method":"accountUnsubscribe","params":[64],"id":13}`
 *    - the params contains the mapper_id to subscribe.
 *    - create a new mapper_id
 *    - for each upstream, find the sub_id corresponding to mapper_id.
 *    - replace params with [sub_id], and send it to upstream.
 *    - send downstream `{"jsonrpc":"2.0","result": true,"id":13}`
 * <- unstream responds, with id corresponding to the id, and result be true.
 *    `{"jsonrpc":"2.0","result": true,"id":14}`
 *    - ignore
 */

/**
 * Possible subscription types.
 */
const subscrptionTypes = [
  "account",
  "block",
  "logs",
  "program",
  "signature",
  "slot",
  "slotsUpdates",
  "root",
  "vote",
];
const subscribe_methods = subscrptionTypes.map((x) => `${x}Subscribe`);
/**
 *
 * @param method
 * @returns
 */
function isSubscribe(method: string) {
  return subscribe_methods.find((x) => x == method) !== undefined;
}
const unsubscribe_methods = subscrptionTypes.map((x) => `${x}Unsubscribe`);
/**
 *
 * @param method
 * @returns
 */
function isUnsubscribe(method: string) {
  return unsubscribe_methods.find((x) => x == method) !== undefined;
}
const notification_methods = subscrptionTypes.map((x) => `${x}Notification`);
/**
 *
 * @param method
 * @returns
 */
function isNotification(method: string) {
  return notification_methods.find((x) => x == method) !== undefined;
}
/**
 *
 * @param msg
 * @returns
 */
function isResponseToSubscribe(msg: WebSocketResponse) {
  return msg.id !== undefined && typeof msg.result === "number";
}

/**
 * Upstream contains a websocket connection to upstream and the mapping between mapper_id and subscription id.
 */
interface Upstream {
  /**
   * upstream websocket connection.
   * upstream will only be created if there is a new client connection.
   */
  upstream: WebSocket | undefined;
  /**
   * url of the websocket
   */
  ws_url: string;
  /**
   * mapping from mapper id and subscription id
   */
  mapper_id_to_sub_id: Map<number, number>;
  /**
   * mapping frrom subscription id to mapper id
   */
  sub_id_to_mapper_id: Map<number, number>;
}

/**
 * StreamMapper maintains the a group of upstreams and group of downstreams and manage the subscriptions.
 */
interface StreamMapper {
  /**
   * upstreams
   */
  upstreams: Upstream[];
  /**
   * downstreams
   */
  downstreams: Map<number, WebSocket>;
  /**
   * current mapper_id
   * each new method call to upstream will be mapped to a new mapper id.
   */
  current_mapper_id: number;
  /**
   * current down stream id.
   */
  current_downstream_id: number;
  /**
   * mapping from mapper_id to the downstream information
   */
  mapper_id_to_downstream: Map<number, { downstream_id: number; downstream_method_id: number }>;
  /**
   * indicating if the mapper_id's response has been sent.
   */
  mapper_id_of_subs: Map<number, boolean>;
  /**
   * indicating if a mapper_id is a unsubscribe
   */
  unsubscribe_ids: Map<number, boolean>;
}

/**
 * create a new StreamMapper
 * @param urls urls to upstreams
 * @returns
 */
function createStreamMapper(urls: string[]) {
  const result: StreamMapper = {
    upstreams: urls.map((url) => {
      const t: Upstream = {
        upstream: undefined,
        ws_url: url,
        mapper_id_to_sub_id: new Map(),
        sub_id_to_mapper_id: new Map(),
      };
      return t;
    }),
    downstreams: new Map(),
    current_mapper_id: 1,
    current_downstream_id: 0,
    mapper_id_to_downstream: new Map(),
    mapper_id_of_subs: new Map(),
    unsubscribe_ids: new Map(),
  };
  return result;
}

/**
 * reset stream mapper
 * @param stream_mapper
 */
function resetStreamMapper(stream_mapper: StreamMapper) {
  stream_mapper.upstreams.forEach((x) => {
    x.mapper_id_to_sub_id.clear();
    x.sub_id_to_mapper_id.clear();
    if (x.upstream !== undefined) {
      x.upstream.close();
    }
    x.upstream = undefined;
  });
  stream_mapper.current_mapper_id = 1;
  stream_mapper.current_downstream_id = 1;
  stream_mapper.mapper_id_to_downstream.clear();
  stream_mapper.downstreams.forEach((v) => {
    v.close();
  });
  stream_mapper.downstreams.clear();
  stream_mapper.mapper_id_of_subs.clear();
  stream_mapper.unsubscribe_ids.clear();
}

/**
 * add web socket to stream mapper
 * @param stream_mapper
 * @param ws
 * @returns id of the added websocket in the stream mapper.
 */
function addDownstream(stream_mapper: StreamMapper, ws: WebSocket) {
  const id = stream_mapper.current_downstream_id;
  stream_mapper.current_downstream_id++;
  stream_mapper.downstreams.set(id, ws);
  return id;
}

/**
 * remove downstream and if no down stream, shut down all upstream connections.
 * @param stream_mapper
 * @param downstream_id
 */
function removeDownstream(stream_mapper: StreamMapper, downstream_id: number) {
  stream_mapper.downstreams.delete(downstream_id);
  if (stream_mapper.downstreams.size == 0) {
    console.log("ws :: zero downstreams, shutting down all upstreams");
    resetStreamMapper(stream_mapper);
  }
}

/**
 * Process a subscribe's response.
 * // `{"jsonrpc":"2.0","result":53,"id":1}`
 * @param stream_mapper
 * @param upstream
 * @param data_string
 * @param msg
 * @returns
 */
function processResponse(
  stream_mapper: StreamMapper,
  upstream: Upstream,
  data_string: string,
  msg: WebSocketResponse,
  index: number
) {
  console.log(`ws -> message subscribe response from upstream ${index}: ${data_string}`);
  // msg id is the mapper_id.
  const mapper_id = msg.id;
  // sub id is in the result.
  const sub_id = msg.result;
  // check if the mapper id's response has been sent.
  const is_sub_sent = stream_mapper.mapper_id_of_subs.get(mapper_id);
  if (is_sub_sent === undefined) {
    console.log(`ws :: ${data_string} doesn't contain a valid sub id ${mapper_id}`);
    return;
  }
  if (typeof sub_id !== "number") {
    console.log(`ws :: subscription id ${sub_id} is not a number in ${data_string}.`);
    return;
  }
  // record this.
  upstream.mapper_id_to_sub_id.set(mapper_id, sub_id);
  upstream.sub_id_to_mapper_id.set(sub_id, mapper_id);

  // if the response to the sub is already sent to the downstream, return.
  if (is_sub_sent) {
    return;
  }

  // send the response to the downstream.
  const downstream_info = stream_mapper.mapper_id_to_downstream.get(mapper_id);
  if (downstream_info === undefined) {
    console.log(`ws :: ${data_string} doesn't contain a valid downstream`);
    return;
  }
  const ws = stream_mapper.downstreams.get(downstream_info.downstream_id);
  if (ws === undefined) {
    console.log(
      `ws :: ${data_string}'s downstream ${downstream_info.downstream_id} doesn't exists`
    );
    return;
  }
  console.log(
    `ws -> sending sub confirmation to ${downstream_info.downstream_id} with ${downstream_info.downstream_method_id}, mapped from ${mapper_id}`
  );
  stream_mapper.mapper_id_of_subs.set(mapper_id, true);
  msg.id = downstream_info.downstream_method_id;
  // for downstream, the subscription id is the mapper_id
  msg.result = mapper_id;
  ws.send(JSON.stringify(msg));
}

/**
 * Process notification
 *
 * @param stream_mapper
 * @param upstream
 * @param data_string
 * @param msg
 * @param index
 * @returns
 */
function processNotification(
  stream_mapper: StreamMapper,
  upstream: Upstream,
  data_string: string,
  msg: WebSocketNotification,
  index: number
) {
  // data may be huge, so turn it off in the string.
  const show_str = JSON.stringify(msg, function (key, value) {
    if (key === "data") {
      return "omitted";
    }
    return value;
  });
  console.log(`ws -> message notification from upstream ${index}: ${show_str}`);

  // get the mapper_id
  const sub_id = msg.params.subscription;
  const mapper_id = upstream.sub_id_to_mapper_id.get(sub_id);
  if (mapper_id === undefined) {
    console.log(`ws :: ${sub_id} is not mapped to a downstream id`);
    return;
  }
  // figure out downstream.
  const downstream_info = stream_mapper.mapper_id_to_downstream.get(mapper_id);
  if (downstream_info === undefined) {
    console.log(`ws :: ${data_string} doesn't contain a valid downstream`);
    return;
  }
  const ws = stream_mapper.downstreams.get(downstream_info.downstream_id);
  if (ws === undefined) {
    console.log(
      `ws :: ${data_string}'s downstream ${downstream_info.downstream_id} doesn't exists`
    );
    return;
  }
  // replace the subscription id with mapper_id, which is the subscription id for the downstream.
  msg.params.subscription = mapper_id;
  console.log(
    `ws -> notify ${downstream_info.downstream_id} with ${mapper_id} from upstream ${index}/${sub_id}`
  );
  ws.send(JSON.stringify(msg));
}

/**
 * Set up the upstream at index of the stream mapper.
 * @param stream_mapper
 * @param index index of the upstream
 */
async function setupUpstream(stream_mapper: StreamMapper, index: number) {
  const upstream = stream_mapper.upstreams[index];
  // if upstream is not setup, set it up now.
  if (upstream.upstream === undefined) {
    console.log(`ws :: connecting to upstream ${upstream.ws_url} at ${index}`);
    upstream.upstream = new WebSocket(upstream.ws_url);
    // on message for the upstream.
    // this must be set up right after creating web socket
    upstream.upstream?.on("message", function (data) {
      // convert the data to string.
      const data_string = data.toString();
      const msg = JSON.parse(data_string);
      if (isResponseToSubscribe(msg)) {
        // response
        processResponse(stream_mapper, upstream, data_string, msg, index);
      } else if (isNotification(msg.method)) {
        // notification
        processNotification(stream_mapper, upstream, data_string, msg, index);
      } else if (msg.result === true && stream_mapper.unsubscribe_ids.has(msg.id)) {
        // unsub response.
        console.log(
          `ws :: ignorning unsubscribe confirmation from upstream ${index}: ${data_string}`
        );
      } else {
        // everything else
        console.log(`ws -> message forward for upstream ${index}: ${data_string}`);
        // forward everything else.
        stream_mapper.downstreams.forEach((ws) => {
          ws.send(data);
        });
      }
    });
  }
  // wait for the upstream to be ready.
  if (upstream.upstream.readyState !== WebSocket.OPEN) {
    await new Promise((resolve) => upstream.upstream?.on("open", resolve));
  }
}

/**
 * Process a subscribe message.
 * `{"jsonrpc":"2.0","method":"accountSubscribe","params":["5oNSm87yBqyKRz2mGqM34xqt2mWKVYvk8CVYKvdnBDBc",{"encoding":"base64","commitment":"processed"}],"id":1}`
 * @param stream_mapper
 * @param downstream_id
 * @param msg
 * @param data_string
 * @returns
 */
function processSubscribe(
  stream_mapper: StreamMapper,
  downstream_id: number,
  msg: WebSocketSubcribe,
  data_string: string
) {
  console.log(`ws :: ${msg.method} is subscribe: ${data_string}`);
  // downstream_method_id
  const downstream_method_id = msg.id;
  if (downstream_method_id === undefined) {
    console.log(`ws :: ${data_string} doesn't contain a valid id`);
    return;
  }
  // create the corresponding mapper_id
  const mapper_id = stream_mapper.current_mapper_id;
  stream_mapper.current_mapper_id++;
  // indicate the mapper_id is not responsed yet.
  stream_mapper.mapper_id_of_subs.set(mapper_id, false);
  // record the downstream info for the mapper id
  stream_mapper.mapper_id_to_downstream.set(mapper_id, {
    downstream_id: downstream_id,
    downstream_method_id: downstream_method_id,
  });

  console.log(
    `ws :: mapping method ${downstream_method_id} of downstream ${downstream_id} to ${mapper_id}`
  );
  // replace the id with the mapper id and send it to all upstreams.
  msg.id = mapper_id;
  const data_to_sent = JSON.stringify(msg);
  stream_mapper.upstreams.forEach((upstream, index) => {
    upstream.upstream?.send(data_to_sent, (err) => {
      if (err !== undefined) {
        console.log(`ws:: sending sub request to ${index} failed: ${err}`);
      }
    });
  });
}

/**
 * Process unsubscribe
 * `{"jsonrpc":"2.0","method":"accountUnsubscribe","params":[64],"id":13}`
 *
 * @param stream_mapper
 * @param msg
 * @param data_string
 * @param ws
 * @returns
 */
function processUnsubscribe(
  stream_mapper: StreamMapper,
  msg: WebSocketUnsubscribe,
  data_string: string,
  ws: WebSocket
) {
  console.log(`ws :: ${msg.method} is unsubscribe: ${data_string}`);
  msg = msg as WebSocketUnsubscribe;
  const params_in = msg.params;
  if (params_in.length !== 1) {
    console.log(`ws :: ${params_in} length not right in ${data_string}`);
    return;
  }
  // send the downstream method id so that response can be sent.
  const downstream_method_id = msg.id;
  // the mapper id to the subscripption.
  const mapper_id = params_in[0];
  // replace the msg's id with a new mapper_id
  msg.id = stream_mapper.current_mapper_id;
  stream_mapper.current_mapper_id++;
  // set the msg.id to true to indicate that this is an unsubscribe method.
  stream_mapper.unsubscribe_ids.set(msg.id, true);
  // send the unsubscribe to all upstreams.
  stream_mapper.upstreams.forEach(({ upstream, mapper_id_to_sub_id }, index) => {
    // the params - the subscription id.
    const sub_id = mapper_id_to_sub_id.get(mapper_id);
    if (sub_id === undefined) {
      console.log(`ws :: ${mapper_id} doesn't have corresponding sub_id at ${index}`);
      return;
    }
    msg.params = [sub_id];
    const to_send = JSON.stringify(msg);
    console.log(`ws -> sending unsubscribe to ${index} with ${to_send}`);
    if (upstream === undefined) {
      console.log(`ws :: upstream at ${index} is not initialized.`);
      return;
    }
    upstream.send(to_send, (err) => {
      if (err !== undefined) {
        console.log(`ws :: failed to send unsubscribe to ${index}`);
      }
    });
  });
  const unsub_response = `{ "jsonrpc": "2.0", "result": true, "id": ${downstream_method_id} }`;
  console.log(`ws -> send downstream unsubscribe confirmation ${unsub_response}`);
  // send the confirmation to client that unsub is successful.
  ws.send(unsub_response);
}

/**
 * run web socket
 * @param port_ws
 * @param ws_urls urls for upstream websocket servers
 */
function runWs(port_ws: number, ws_urls: string[]) {
  const ws_server = new WebSocketServer({ port: port_ws }, () => {
    console.log(`ws started at ${port_ws}`);
  });

  const stream_mapper = createStreamMapper(ws_urls);

  ws_server.on("connection", async (ws) => {
    const downstream_id = addDownstream(stream_mapper, ws);
    ws.on("message", async (data) => {
      await Promise.all(
        stream_mapper.upstreams.map(async (upstream, i) => {
          await setupUpstream(stream_mapper, i);
        })
      );
      const data_string = data.toString();
      const msg = JSON.parse(data_string);
      console.log(`ws <- message received: ${data_string}`);

      if (isSubscribe(msg.method)) {
        processSubscribe(stream_mapper, downstream_id, msg, data_string);
      } else if (isUnsubscribe(msg.method)) {
        processUnsubscribe(stream_mapper, msg, data_string, ws);
      } else {
        stream_mapper.upstreams.forEach(({ upstream }, index) => {
          upstream?.send(data, (err) => {
            if (err !== undefined) {
              console.log(`ws :: failed to send unsubscribe to ${index}`);
            }
          });
        });
      }
    });

    ws.on("close", () => {
      console.log(`ws :: removing downstream_id ${downstream_id}`);
      removeDownstream(stream_mapper, downstream_id);
    });
  });
}

/////////////////////////////////////////////////////////////////////////////
// rpc

/**
 *
 * @param urls urls of the upstream rpc servers
 * @param port port number for the rpc server.
 */
function runRpc(urls: string[], port: number) {
  const app = express();
  app.use(express.json());

  let rpc_slot_number = 0;

  app.post("/", async (request, response) => {
    console.log(`rpc <- request: ${JSON.stringify(request.body)}`);
    let processed = false;
    const body = JSON.stringify(request.body);
    urls.forEach((rpc_url, index) => {
      if (request.body?.method === "sendTransaction") {
        if (index > 0) {
          console.log(
            `URL at ${index} will not be called since only one sendTransaction can be sent`
          );
          return;
        }
      }
      fetch(rpc_url, {
        method: "POST",
        body: body,
        headers: {
          "content-type": "application/json",
        },
      }).then(
        async (res) => {
          const res_json = await res.json();
          if (processed) {
            return;
          }
          const this_slot = res_json.result?.context?.slot;
          // a newer version
          const should_respond = typeof this_slot === "undefined" || this_slot >= rpc_slot_number;
          if (!should_respond) {
            console.log(
              `rpc -> response: ${this_slot} may be less than the current max known slot: ${rpc_slot_number}`
            );
          }
          processed = true;
          rpc_slot_number = this_slot;
          console.log(
            `rpc -> response status by url ${index}: ${res.status} ${
              res.statusText
            } ${JSON.stringify(res.headers)}`
          );
          console.log(`rpc -> response body: ${JSON.stringify(res_json)}`);
          response.status(res.status).send(res_json);
        },
        (rejected_reason) => {
          console.log(`request at ${index} is rejected: ${rejected_reason}`);
        }
      );
    });
  });

  app.listen(port, () => {
    console.log(`rpc started at ${port}`);
  });
}

async function main() {
  // get the arguments
  const argp = new ArgumentParser({ description: "rpc multiplexer for solana." });
  argp.add_argument("-u", "--url", { action: "append", required: true });
  argp.add_argument("-p", "--rpc_port", { type: "int", required: false, default: 8899 });
  argp.add_argument("--ws_port", { type: "int" });
  const args = argp.parse_args();

  const urls = (args.url as string[]).map(getUrl);
  const port: number = args.rpc_port;
  const port_ws: number = args.ws_port || port + 1;
  // log the destinations.
  console.log(`RPC urls: ${urls.map((x) => x.rpc_url)}`);
  console.log(`WebSocket urls: ${urls.map((x) => x.ws_url)}`);

  runRpc(
    urls.map(({ rpc_url }) => rpc_url),
    port
  );

  // set up RPC.
  const ws_urls = urls.map(({ ws_url }) => ws_url);

  runWs(port_ws, ws_urls);
}

main();
