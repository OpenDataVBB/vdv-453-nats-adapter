# vdv-453-nats-adapter

**Sends realtime public transport data from a [VDV-453/VDV-454 API](https://www.vdv.de/i-d-s-downloads.aspx) to a [NATS message broker](https://docs.nats.io/)**, so that it can be easily consumed by other applications. Uses [`vdv-453-client`](https://github.com/OpenDataVBB/vdv-453-client) underneath.

![ISC-licensed](https://img.shields.io/github/license/OpenDataVBB/vdv-453-nats-adapter.svg)


## Installation

```shell
npm install -g OpenDataVBB/vdv-453-nats-adapter
```


## Getting Started

Please note the limitations imposed by the underlying library [`vdv-453-client`](https://github.com/OpenDataVBB/vdv-453-client):

> **A client for [VDV-453 v2.3.2b](https://web.archive.org/web/20231208122259/https://www.vdv.de/453v2.3.2-sds.pdf.pdfx?forced=false)/[VDV-454 v1.2.2](https://web.archive.org/web/20231208122259/https://www.vdv.de/454v1.2.2-sds.pdf.pdfx?forced=false) (from 2013) systems.** Can be used to connect to German public transport realtime data backends (*Datendrehscheiben*).
> 
> *Note:* This client supports neither the latest 2.x spec versions ([VDV-453 v2.6.1](https://www.vdv.de/vdv-schrift-453-v2.6.1-de.pdfx?forced=true)/[VDV-454 v2.2.1](https://www.vdv.de/454v2.2.1-sd.pdfx?forced=true)) nor the latest 3.x spec versions ([VDV-453 v3.0](https://www.vdv.de/downloads/4337/453v3.0%20SDS/forced)/[VDV-454 v3.0](https://www.vdv.de/downloads/4336/454v3.0%20SDS/forced)).

With the organisation providing the VDV 453 API, you will have to agree upon a *Leitstellenkennung*, which is a bit like an HTTP User-Agent:

> 6.1.3 Leitstellenkennung
>
> Um Botschaften verschiedener Kommunikationspartner innerhalb eines Dienstes unterscheiden zu können, enthält jede Nachricht eine eindeutige Leitstellenkennung (Attribut `Sender`) des nachfragenden Systems. […]

Then, configure access to the VDV-453 system and run the adapter:

```bash
send-vdv-453-data-to-nats \
	# must have a trailing `/`
	--endpoint 'http://example.org:1234/api/' \
	# as agreed-upon between the VDV-453 API's operator and you
	--leitstelle '…'
```


## Usage

```
Usage:
    send-vdv-453-data-to-nats [options] <service>
Notes:
    Valid values for `service`:
    - `AUS` subscribes to the VDV-454 AUS service containing network-wide realtime data.
Options:
	--leitstelle              -l  VDV-453 Leitstellenkennung, a string identifying this
	                              client, a bit like an HTTP User-Agent. Must be agreed-
	                              upon with the provider of the VDV-453 API.
	                              Default: $VDV_453_LEITSTELLE
	--endpoint                -e  HTTP(S) URL of the VDV-453 API.
	                              Default: $VDV_453_ENDPOINT
	--port                    -p  Port to listen on. VDV-453 requires the *client* to run
	                              an HTTP server that the VDV-453 API can call.
	                              Default: $PORT, otherwise 3000
	--expires                     Set the subscription's expiry date & time. Must be an
	                              ISO 8601 date+time string or a UNIX epoch/timestamp.
	                              Default: now + 1h
	--nats-servers                NATS server(s) to connect to.
	                              Default: $NATS_SERVERS
	--nats-user                   User to use when authenticating with NATS server.
	                              Default: $NATS_USER
	--nats-client-name            Name identifying the NATS client among others.
	                              Default: vdv453-1-${randomHex(4)}
Exit Codes:
	1 – generic and/or unexpected error
	2 – operation canceled
	3 – VDV-453 API error
Examples:
    send-vdv-453-data-to-nats --expires never AUS
```


## Related

- [vdv-453-client](https://github.com/OpenDataVBB/vdv-453-client) – A client for VDV-453/VDV-454 systems.


## Contributing

If you have a question or need support using `vdv-453-nats-adapter`, please double-check your code and setup first. If you think you have found a bug or want to propose a feature, use [the issues page](https://github.com/OpenDataVBB/vdv-453-nats-adapter/issues).
