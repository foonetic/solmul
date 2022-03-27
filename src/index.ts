import express from "express";
import { WebSocketServer } from "ws";

async function main() {
  const app = express();
  const port = 8899;
  const port_ws = port + 1;
  app.get("/", (request, response) => {
    console.log(`${JSON.stringify(request)}`);
    response.send("hello world!");
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
