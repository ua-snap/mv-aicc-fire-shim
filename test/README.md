# How to test this?

## Bane

Install `[bane](https://github.com/danielwellman/bane)`.  Modify the URLs used in this app to point at `localhost:3000`, then try:

```
10026  bane 3001 CloseAfterPause
10028  bane 3001 DelugeResponse
10029  bane 3001 EchoResponse
10031  bane 3001 NeverRespond
10032  bane 3001 NewlineResponse
10036  bane 3001 TimeoutInListenQueue
10037  bane 3001 HttpRefuseAllCredentials
10038  bane 3001 SlowResponse
```

## Serving malformed GeoJSON

```
cd test
python -m SimpleHTTPServer
```

Edit URLs to point at:

 * `http://localhost:3000/empty.json`
 * `http://localhost:3000/garbage.json`
 * `http://localhost:3000/malformed.json`

## Testing unexpected HTTP response codes

Change URLs to:

 * `http://httpstat.us/500`
 * `http://httpstat.us/404`
