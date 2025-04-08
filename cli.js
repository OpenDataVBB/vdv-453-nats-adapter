#!/usr/bin/env node

import {parseArgs} from 'node:util'

// todo: use import assertions once they're supported by Node.js & ESLint
// https://github.com/tc39/proposal-import-assertions
import {createRequire} from 'node:module'
import {PREFIX as NATS_CLIENT_NAME_PREFIX} from './lib/nats.js'
const require = createRequire(import.meta.url)
const pkg = require('./package.json')

const {
	values: flags,
	positionals: args,
} = parseArgs({
	options: {
		'help': {
			type: 'boolean',
			short: 'h',
		},
		'version': {
			type: 'boolean',
			short: 'v',
		},
		'leitstelle': {
			type: 'string',
			short: 'l',
		},
		'their-leitstelle': {
			type: 'string',
			short: 'L',
		},
		'endpoint': {
			type: 'string',
			short: 'e',
		},
		'port': {
			type: 'string',
			short: 'p',
		},
		'nats-servers': {
			type: 'string',
		},
		'nats-user': {
			type: 'string',
		},
		'nats-client-name': {
			type: 'string',
		},
		'expires': {
			type: 'string',
		},
		'aus-manual-fetch-interval': {
			type: 'string',
		},
	},
	allowPositionals: true,
})

if (flags.help) {
	process.stdout.write(`
Usage:
    send-vdv-453-data-to-nats [options] <service>
Notes:
    Valid values for \`service\`:
    - \`AUS\` subscribes to the VDV-454 AUS service containing network-wide realtime data.
Options:
	--leitstelle                 -l  VDV-453 Leitstellenkennung, a string identifying this
	                                 client, a bit like an HTTP User-Agent. Must be agreed-
	                                 upon with the provider of the VDV-453 API.
	                                 Default: $VDV_453_LEITSTELLE
	--their-leitstelle           -L  VDV-453 Leitstellenkennung of the server. Must be agreed-
	                                 upon with the provider of the VDV-453 API.
	                                 Default: $VDV_453_THEIR_LEITSTELLE
	--endpoint                   -e  HTTP(S) URL of the VDV-453 API.
	                                 Default: $VDV_453_ENDPOINT
	--port                       -p  Port to listen on. VDV-453 requires the *client* to run
	                                 an HTTP server that the VDV-453 API can call.
	                                 Default: $PORT, otherwise 3000
	--nats-servers                   NATS server(s) to connect to.
	                                 Default: $NATS_SERVERS
	--nats-user                      User to use when authenticating with NATS server.
	                                 Default: $NATS_USER
	--nats-client-name               Name identifying the NATS client among others.
	                                 Default: ${NATS_CLIENT_NAME_PREFIX}\${randomHex(4)}
AUS-specific Options:
	--expires                        Set the AUS subscription's expiry date & time. Must be
	                                 an ISO 8601 date+time string or a UNIX epoch/timestamp.
	                                 Default: now + 1h
	--aus-manual-fetch-interval      How often to *manually* fetch the data of an AUS
	                                 subscription, in milliseconds.
	                                 Usually, the server should notify the client about new
	                                 data, but some may not.
	                                 Default: 30_000 (30s)
Exit Codes:
	1 – generic and/or unexpected error
	2 – operation canceled
	3 – VDV-453 API error
Examples:
    send-vdv-453-data-to-nats --expires never AUS
\n`)
	process.exit(0)
}

if (flags.version) {
	process.stdout.write(`${pkg.name} v${pkg.version}\n`)
	process.exit(0)
}

import {DateTime, SystemZone} from 'luxon'
import {
	SERVICES,
	Vdv453ApiError,
} from 'vdv-453-client'
import {sendVdv453DataToNats} from './index.js'

const STATUS_CODE_CANCELED = 2
const STATUS_CODE_VDV_ERROR = 3

const DEBUG = process.env.VDV_453_DEBUG === 'true'

const abortWithError = (err) => {
	console.error(err)
	process.exit(1)
}

const cfg = {}
const subscriptionOpts = Object.fromEntries(
	SERVICES.map(svc => [svc, {}])
)
const opt = {
	natsOpts: {},
}

const validServices = Object.keys(SERVICES).filter(key => !/^\d+$/.test(key)) // ignore array indices
if (args.length === 0) {
	abortWithError('missing service')
}
const [serviceArg] = args
if (!serviceArg) {
	abortWithError(`missing/empty service`)
}
if (!validServices.includes(serviceArg)) {
	abortWithError(`invalid service, must be one of ${validServices.join(', ')}`)
}
// todo: use `SERVICES[serviceArg]` (requires breaking changes in index.js)
const service = serviceArg

if ('leitstelle' in flags) {
	cfg.leitstelle = flags.leitstelle
} else if ('VDV_453_LEITSTELLE' in process.env) {
	cfg.leitstelle = process.env.VDV_453_LEITSTELLE
} else {
	abortWithError('missing/empty --leitstelle/-l/$VDV_453_LEITSTELLE')
}
if ('their-leitstelle' in flags) {
	cfg.theirLeitstelle = flags['their-leitstelle']
} else if ('VDV_453_THEIR_LEITSTELLE' in process.env) {
	cfg.theirLeitstelle = process.env.VDV_453_THEIR_LEITSTELLE
} else {
	abortWithError('missing/empty --their-leitstelle/-L/$VDV_453_THEIR_LEITSTELLE')
}

if ('endpoint' in flags) {
	cfg.endpoint = flags.endpoint
} else if ('VDV_453_ENDPOINT' in process.env) {
	cfg.endpoint = process.env.VDV_453_ENDPOINT
} else {
	abortWithError('missing/empty --endpoint/-e/$VDV_453_ENDPOINT')
}

if ('port' in flags) {
	cfg.port = parseInt(flags.port)
} else if ('PORT' in process.env) {
	cfg.port = parseInt(process.env.PORT)
} else {
	cfg.port = 3000
}

if ('nats-servers' in flags) {
	opt.natsOpts.servers = flags['nats-servers'].split(',')
}
if ('nats-user' in flags) {
	opt.natsOpts.user = flags['nats-user']
}
if ('nats-client-name' in flags) {
	opt.natsOpts.name = flags['nats-client-name']
}

const parseExpiresFlag = (expiresFlag) => {
	if (/^\d+$/.test(expiresFlag)) { // UNIX epoch/timestamp
		return parseInt(expiresFlag)
	} else {
		const expires = DateTime.fromISO(expiresFlag, {setZone: true})
		if (expires.zone instanceof SystemZone) {
			abortWithError('--expires ISO 8601 must specify a time zone (offset)')
		}
		return expires.toUnixInteger()
	}
}
// todo [breaking]: rename to --aus-expires?
if ('expires' in flags) {
	subscriptionOpts.expires = parseExpiresFlag(flags.expires)
} else {
	// now + 1h
	subscriptionOpts.expires = (Date.now() / 1000 | 0) + 60 * 60
}

if ('aus-manual-fetch-interval' in flags) {
	opt.ausManualFetchInterval = parseInt(flags['aus-manual-fetch-interval'])
	if (!Number.isInteger(opt.ausManualFetchInterval)) {
			abortWithError('--aus-manual-fetch-interval must be an integer')
	}
}

cfg.subscriptions = [
	{
		...subscriptionOpts,
		service,
	},
]

// todo [breaking]: create pino logger here, pass it in?
let stop = async () => {} // no-op
try {
	const _ = await sendVdv453DataToNats(cfg, opt)
	stop = _.stop
} catch (err) {
	// todo: special handling for Vdv453HttpError too?
	if (!DEBUG && err instanceof Vdv453ApiError) {
		process.stderr.write(err.message + '\n')
		process.exit(STATUS_CODE_VDV_ERROR)
	}
	abortWithError(err)
}

let shouldHardExit = false
process.on('SIGINT', () => {
	// try to soft-exit on first SIGINT, then hard-exit with status code 1
	if (shouldHardExit) {
		process.exit(STATUS_CODE_CANCELED)
	}
	shouldHardExit = true

	process.stderr.write('unsubscribing\n')
	// todo: set timeout?
	stop().catch(abortWithError)
})
