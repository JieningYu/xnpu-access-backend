# xnpu-access-backend

HTTP API for streaming output over SSH.

## Request

Submit a length and prompt with a raw `POST` body. The request length is a
non-negative integer supplied in the required `len` query parameter:

```http
POST /?len=1024
Content-Type: text/plain

Write a hello world program in Rust.
```

The `len` value is available to the command as `%i`.

The response is `202 Accepted`. Its body is a one-time request ID, and
`Location` points to the WebSocket endpoint:

```http
202 Accepted
Location: /ws/1

1
```

## Response stream

Connect to the returned `Location` with a WebSocket client:

```text
ws://localhost:3000/ws/1
```

Command stdout is sent as binary WebSocket messages as it arrives. The server
closes the WebSocket when the command finishes.

Before closing, the server sends one text message containing generation metrics
found in command stderr:

```json
{
  "type": "generation_metrics",
  "available": true,
  "prefill": {
    "tokens": 14,
    "duration_ms": 1037.0,
    "tokens_per_second": 13.5
  },
  "decode": {
    "tokens": 50,
    "duration_ms": 4221.26,
    "tokens_per_second": 11.84
  }
}
```

If no metrics are reported, `available` is `false`:

```json
{"type":"generation_metrics","available":false}
```

- Close code `1011`: command failed or exited without a status.
- Each request ID can be used once.

## Example

```sh
id=$(curl -s -X POST --data-binary 'Write hello world in Rust.' \
  'http://localhost:3000/?len=1024')

curl --no-buffer "ws://localhost:3000/ws/$id"
```

## Test mode

Run the API without an SSH host:

```sh
cargo run -- --test-mode
```

Each WebSocket returns a short fake response containing the submitted prompt.
The POST request and WebSocket flow are otherwise unchanged.

## Notes

- Start the server against the SSH host using the CLI options shown by
  `xnpu-access-backend --help`.
- In `--cmd`, `%i` is replaced with the `len` query parameter and `%s` is
  replaced with the client prompt.
- HTTP `POST` is used only to submit the prompt; output is delivered over
  WebSocket.
