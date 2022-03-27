import express from "express";
import WebSocket, { WebSocketServer } from "ws";
import fetch from "cross-fetch";

async function main() {
  const app = express();
  app.use(express.json());

  const port = 8899;
  const port_ws = port + 1;
  const forwarding_url = "http://127.0.0.1:37269";
  const forwarding_url_ws = "ws://127.0.0.1:37270";

  app.post("/", async (request, response) => {
    console.log(`rpc <- request: ${JSON.stringify(request.body, undefined, 2)}`);
    const res = await fetch(forwarding_url, {
      method: "POST",
      body: JSON.stringify(request.body),
      headers: {
        "content-type": "application/json",
      },
    });
    console.log(
      `rpc -> response status: ${res.status} ${res.statusText} ${JSON.stringify(res.headers)}`
    );
    const res_json = await res.json();
    console.log(`rpc -> response body: ${JSON.stringify(res_json, undefined)}`);
    response.status(res.status).send(res_json);
  });

  app.listen(port, () => {
    console.log(`rpc started at ${port}`);
  });

  const ws_server = new WebSocketServer({ port: port_ws }, () => {
    console.log(`ws started at ${port_ws}`);
  });

  ws_server.on("connection", async (ws) => {
    const ws_client = new WebSocket(forwarding_url_ws);
    ws.on("message", async (data) => {
      if (ws_client.readyState !== ws_client.OPEN) {
        await new Promise((resolve) => ws_client.on("open", resolve));
      }
      console.log(`ws <- message received: ${data.toString()}`);
      ws_client.send(data, (err) => {
        if (err !== undefined) {
          console.log(`failed to send data: ${err}`);
        }
      });
    });

    ws_client.on("message", (data) => {
      console.log(`ws -> message response: ${data.toString()}`);
      ws.send(data);
    });

    ws.on("close", () => {
      console.log("closing down the web socket forwarder");
      ws_client.close();
    });
  });
}

main();
