# xnpu-access-backend

HTTP API for streaming output over SSH.

## Request

Submit a prompt with a raw `POST` body:

```http
POST /
Content-Type: text/plain

Write a hello world program in Rust.
```

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

- Close code `1011`: command failed or exited without a status.
- Each request ID can be used once.

## Example

```sh
id=$(curl -s -X POST --data-binary 'Write hello world in Rust.' \
  http://localhost:3000/)

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
- The client prompt replaces `%s` in `--cmd` and is shell-escaped before use.
- HTTP `POST` is used only to submit the prompt; output is delivered over
  WebSocket.
