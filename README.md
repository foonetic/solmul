# **solmul** a Solana rpc Multiplexer

`solmul` is a **sol**ana rpc **mul**tiplexer, which sends the same requests to multiple validators.

- for all rpc calls except send transaction, the first response is sent back.

- for all subscription calls, the notification is inspected by looking at the slot number - and
  only the notification with slot greater than the last seen slot number is sent back.

## Installation

```bash
go get github.com/foonetic/solmul@latest
```

## run the binary

```bash
solmul -u mainnet-beta -u mainnet-beta-serum
```
