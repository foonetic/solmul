import express from "express";
import WebSocket, { WebSocketServer } from "ws";
import fetch from "cross-fetch";
import { ArgumentParser } from "argparse";
import { URL } from "url";

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

interface WebSocketMethodCall {
  id: number;
  method: string;
}

interface WebSocketSubcribe extends WebSocketMethodCall {
  params: any;
}

interface WebSocketUnsubscribe extends WebSocketMethodCall {
  params: number[];
}

interface WebSocketNotification extends WebSocketMethodCall {
  params: {
    subscription: number;
  };
}

interface WebSocketResponse {
  id: number;
  result: number | boolean;
}

interface WsForwarder {
  forwarder: WebSocket;
  client_id_to_sub: Map<number, number>;
  sub_to_client_id: Map<number, number>;
}

function newWsForwarder(url: string) {
  const result: WsForwarder = {
    forwarder: new WebSocket(url),
    client_id_to_sub: new Map(),
    sub_to_client_id: new Map(),
  };
  return result;
}

async function waitForForwarders(forwarders: WsForwarder[]) {
  await Promise.all(
    forwarders.map(async ({ forwarder }) => {
      if (forwarder.readyState !== forwarder.OPEN) {
        await new Promise((resolve) => forwarder.on("open", resolve));
      }
    })
  );
}

/**
 * -> user send someSubscribe, with id
 *    `{"jsonrpc":"2.0","method":"accountSubscribe","params":["5oNSm87yBqyKRz2mGqM34xqt2mWKVYvk8CVYKvdnBDBc",{"encoding":"base64","commitment":"processed"}],"id":1}`
 *    record this id in subscriptions
 *    forward this all upstreams
 * <- upstream responds, with id corresponding to the id in the request, and `result` field with a number, which is the subscrption id.
 *    `{"jsonrpc":"2.0","result":53,"id":1}`
 *    record the result in the subscriptions map to the corresponding index of the upstream, and record the result->id map in the upstream forwarder.
 * <- upstream notifies, with subscription id
 *    `{"jsonrpc":"2.0","method":"signatureNotification","params":{"result":{"context":{"slot":112513},"value":{"err":null}},"subscription":55}}`
 * -> user unsubscribes
 *    `{"jsonrpc":"2.0","method":"accountUnsubscribe","params":[64],"id":13}`
 *    the params contain the subscription id, and the id for the action.
 * <- unstream responds, with id corresponding to the id, and result be true.
 *    `{"jsonrpc":"2.0","method":"accountUnsubscribe","params":[65],"id":14}`
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
function isSubscribe(method: string) {
  return subscribe_methods.find((x) => x == method) !== undefined;
}

const unsubscribe_methods = subscrptionTypes.map((x) => `${x}Unsubscribe`);
function isUnsubscribe(method: string) {
  return unsubscribe_methods.find((x) => x == method) !== undefined;
}
const notification_methods = subscrptionTypes.map((x) => `${x}Notification`);
function isNotification(method: string) {
  return notification_methods.find((x) => x == method) !== undefined;
}

/**
 * Process a subscribe message.
 * `{"jsonrpc":"2.0","method":"accountSubscribe","params":["5oNSm87yBqyKRz2mGqM34xqt2mWKVYvk8CVYKvdnBDBc",{"encoding":"base64","commitment":"processed"}],"id":1}`
 * this will add an empty number[] to subs's mapping and send the subscription method to the upstreams.
 * @param forwarders the upstream websocket to send the subscribe  request.
 * @param msg the msg parsed.
 * @param subs the id to subscribution id mapping.
 * @returns
 */
function processSubscribe(
  forwarders: WsForwarder[],
  msg: WebSocketSubcribe,
  subs: Map<number, number[]>
) {
  const id = msg.id;
  if (id === undefined) {
    console.log(`ws :: id in msg for subscribe is null: ${JSON.stringify(msg)}`);
    return;
  }
  console.log(`ws :: setting up subscription for id ${id}`);
  subs.set(id, []);
  forwarders.forEach(async ({ forwarder }, index) => {
    forwarder.send(JSON.stringify(msg), (err) => {
      if (err !== undefined) {
        console.log(`ws :: failed to send data: ${err} at ${index}`);
      }
    });
  });
}

function isResponse(msg: WebSocketResponse) {
  return msg.id !== undefined && typeof msg.result === "number";
}

/**
 * Process response to subscribe
 * `{"jsonrpc":"2.0","result":53,"id":1}`
 * the result will be pushed into subs map. And if the subs map is empty, send a response to downstream with result replaced with sub id.
 * also, set the corresponding client id <-> sub id mappin in the forwarder.
 * @param msg
 * @param forwarder
 * @param ws
 * @param subs
 * @returns
 */
function processSubscribeResponse(
  msg: WebSocketResponse,
  client_id_to_sub: Map<number, number>,
  sub_to_client_id: Map<number, number>,
  ws: WebSocket,
  subs: Map<number, number[]>
) {
  const id = msg.id;
  const result = msg.result;
  const got_ids = subs.get(id);
  if (got_ids !== undefined) {
    if (typeof result !== "number") {
      console.log(`ws :: get a non-number subscription id ${JSON.stringify(msg)}`);
      return;
    }
    client_id_to_sub.set(id, result);
    sub_to_client_id.set(result, id);
    if (got_ids.length === 0) {
      got_ids.push(result);
      msg.result = id;
      const to_send = JSON.stringify(msg);
      console.log(`ws :: ${id} responds with ${to_send}`);
      ws.send(to_send);
    } else {
      console.log(`ws :: ${id} already responsed`);
    }
  } else {
    console.log(`cannot find ${id} in mapping`);
  }
}

function processNotification(
  sub_to_client_id: Map<number, number>,
  msg: WebSocketNotification,
  ws: WebSocket
) {
  const sub_id = msg.params.subscription;
  const client_id = sub_to_client_id.get(sub_id);
  if (typeof client_id === "undefined") {
    console.log(
      `ws :: ${sub_id} is not found in mapping. cannot forward the msg: ${JSON.stringify(msg)}`
    );
    return;
  }

  msg.params.subscription = client_id;

  console.log(`ws :: forwarding notification: ${client_id} from ${sub_id}`);
  ws.send(JSON.stringify(msg));
}

/**
 * Process an unsubscribe message
 * `{"jsonrpc":"2.0","method":"accountUnsubscribe","params":[64],"id":13}`
 * @param forwarders
 * @param msg
 * @param subs
 * @returns
 */
function processUnsubscribe(
  forwarders: WsForwarder[],
  msg: WebSocketUnsubscribe,
  subs: Map<number, number[]>,
  ws: WebSocket
) {
  const params_in = msg.params;
  if (params_in.length !== 1) {
    console.log(`length of params is not right ${JSON.stringify(msg)}`);
    return;
  }
  const client_id = params_in[0];

  forwarders.forEach(({ forwarder, client_id_to_sub }, index) => {
    const sub_id = client_id_to_sub.get(client_id);
    console.log(`${JSON.stringify(client_id_to_sub)}`);
    if (typeof sub_id === "undefined") {
      console.log(`unknown client id ${client_id}`);
      return;
    }
    msg.params = [sub_id];
    const to_send = JSON.stringify(msg);
    console.log(`ws :: sending unsub to ${index} with ${to_send}`);
    forwarder.send(to_send, (err) => {
      if (err !== undefined) {
        console.log(`ws :: failed to send data: ${err} at ${index}`);
      }
    });
  });
  ws.send(JSON.stringify({ jsonrpc: "2.0", result: true, id: msg.id }));

  subs.delete(client_id);
}

function runWs(port_ws: number, ws_urls: string[]) {
  const ws_server = new WebSocketServer({ port: port_ws }, () => {
    console.log(`ws started at ${port_ws}`);
  });

  ws_server.on("connection", async (ws) => {
    const subscriptions: Map<number, number[]> = new Map();
    const forwarders = ws_urls.map((ws_url) => newWsForwarder(ws_url));
    ws.on("message", async (data) => {
      await waitForForwarders(forwarders);

      const msg = JSON.parse(data.toString());
      console.log(`ws <- message received: ${data.toString()}`);

      if (isSubscribe(msg.method)) {
        console.log(`ws :: ${msg.method} is subscribe`);
        processSubscribe(forwarders, msg, subscriptions);
      } else if (isUnsubscribe(msg.method)) {
        console.log(`ws :: ${msg.method} is unsubscribe`);
        processUnsubscribe(forwarders, msg, subscriptions, ws);
      } else {
        forwarders.forEach(({ forwarder }) => {
          forwarder.send(data, (err) => {
            if (err !== undefined) {
              console.log(`failed to send data: ${err}`);
            }
          });
        });
      }
    });

    forwarders.forEach(({ forwarder, client_id_to_sub, sub_to_client_id }) => {
      forwarder.on("message", (data) => {
        const data_string = data.toString();
        const msg = JSON.parse(data_string);
        console.log(`ws -> message response: ${data_string}`);
        if (isResponse(msg)) {
          console.log(`ws :: ${data_string} is response`);
          processSubscribeResponse(msg, client_id_to_sub, sub_to_client_id, ws, subscriptions);
        } else if (isNotification(msg.method)) {
          console.log("ws :: is notification");
          processNotification(sub_to_client_id, msg, ws);
        } else {
          ws.send(data);
        }
      });
    });

    ws.on("close", () => {
      console.log("closing down the web socket forwarder");
      forwarders.forEach(({ forwarder }) => {
        forwarder.close();
      });
    });
  });
}

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
