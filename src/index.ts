import express from "express";
import { WebSocketServer } from "ws";
import fetch from "cross-fetch";

async function main() {
  const app = express();
  app.use(express.json());

  const port = 8899;
  const port_ws = port + 1;
  const forwarding_url = "http://127.0.0.1:37269";

  app.post("/", async (request, response) => {
    console.log(`getting request: ${JSON.stringify(request.body, undefined, 2)}`);
    const res = await fetch(forwarding_url, {
      method: "POST",
      body: JSON.stringify(request.body),
      headers: {
        "content-type": "application/json",
      },
    });
    console.log(
      `response status: ${res.status} ${res.statusText} ${JSON.stringify(
        res.headers,
        undefined,
        2
      )}`
    );
    const res_json = await res.json();
    console.log(`${JSON.stringify(res_json)}`);
    response.status(res.status).send(res_json);
  });

  app.listen(port, () => {
    console.log(`started at ${port}`);
  });

  const wss = new WebSocketServer({ port: port_ws });

  wss.on("connection", (ws) => {
    ws.on("message", (data, is_binary) => {
      if (is_binary) {
        console.log(`${data}`);
      }
      ws.send(`your data ${data}`, (err) => {
        if (err !== null) {
          console.log(`error: ${err}`);
        }
      });
    });
  });
}

main();
