# Payload sizes

## Body sizes (bytes)

Encoded message only, no headers or framing. JSON gzip is for discussion; the REST API does not compress.

| scenario | request proto | request JSON | JSON/proto | request JSON gzip | response proto | response JSON | JSON/proto | response JSON gzip |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| small | 85 | 159 | 1.87x | 155 | 44 | 116 | 2.64x | 131 |
| medium | 618 | 689 | 1.11x | 399 | 50 | 167 | 3.34x | 148 |
| large | 3702 | 3773 | 1.02x | 422 | 50 | 167 | 3.34x | 148 |
| batch | 8500 | 16012 | 1.88x | 2308 | 4600 | 11713 | 2.55x | 2286 |

## Wire bytes per request (average over 1000 requests on a warm connection)

Counted on the server socket: includes HTTP/1.1 headers, HTTP/2 frames + HPACK, gRPC framing and trailers; excludes connection setup.

| scenario | protocol | client→server | server→client | total |
|---|---|---:|---:|---:|
| small | gRPC | 163 | 127 | 290 |
| small | REST/HTTP1.1 | 346 | 232 | 578 |
| small | REST/h2c | 186 | 140 | 326 |
| medium | gRPC | 696 | 133 | 829 |
| medium | REST/HTTP1.1 | 876 | 283 | 1159 |
| medium | REST/h2c | 717 | 192 | 909 |
| large | gRPC | 3780 | 133 | 3913 |
| large | REST/HTTP1.1 | 3961 | 283 | 4244 |
| large | REST/h2c | 3801 | 200 | 4000 |
| batch | gRPC | 8578 | 4683 | 13261 |
| batch | REST/HTTP1.1 | 16207 | 11849 | 28056 |
| batch | REST/h2c | 16078 | 11770 | 27848 |
