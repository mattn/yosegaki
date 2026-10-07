# yosegaki

Collaborative editing server for Vim. The client is [vim-yosegaki](https://github.com/mattn/vim-yosegaki).

One server runs many sessions at once. Each session has a host who chooses public (anyone with the id can view, editors need the host's approval) or private (guests need the host's approval to enter). Edits are merged with operational transformation.

## Installation

Download a binary from [Releases](https://github.com/mattn/yosegaki/releases), or

```
go install github.com/mattn/yosegaki@latest
```

The same binary is needed on every machine running Vim: `yosegaki connect` relays between Vim (job stdio) and the server (WebSocket over TLS), since Vim channels cannot speak either.

## Docker

```
docker run -d -p 8080:8080 ghcr.io/mattn/yosegaki
```

## Usage

```
yosegaki serve -addr :8080      # run the server
yosegaki connect URL            # stdio bridge used by vim-yosegaki
yosegaki list URL               # print public sessions as JSON
```

Endpoints:

- `/ws` WebSocket for clients
- `/sessions` public sessions as JSON

Run it behind a TLS reverse proxy that passes WebSocket upgrades, for example with nginx:

```nginx
location / {
    proxy_pass http://127.0.0.1:8080;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_read_timeout 1h;
}
```

Sessions live in memory and end when the host disconnects.

## License

MIT

## Author

Yasuhiro Matsumoto (a.k.a. mattn)
