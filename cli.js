#!/usr/bin/env node

import {parseArgs} from 'node:util'

// todo: use import assertions once they're supported by Node.js & ESLint
// https://github.com/tc39/proposal-import-assertions
import {createRequire} from 'node:module'
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
		'endpoint': {
			type: 'string',
			short: 'e',
		},
		'expires': {
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
	--leitstelle              -l  VDV-453 Leitstellenkennung, a string identifying this
	                              client, a bit like an HTTP User-Agent. Must be agreed-
	                              upon with the provider of the VDV-453 API.
	                              Default: $VDV_453_LEITSTELLE
	--endpoint                -e  HTTP(S) URL of the VDV-453 API.
	                              Default: $VDV_453_ENDPOINT
	--port                    -p  Port to listen on. VDV-453 requires the client to run an
	                              HTTP server that the VDV-453 API can call.
	                              Default: $PORT, otherwise 3000
    --expires                     Set the subscription's expiry date & time. Must be an
                                  ISO 8601 date+time string or a UNIX epoch/timestamp.
                                  Default: now + 1h
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
import {Vdv453ApiError} from 'vdv-453-client'
import {sendVdv453DataToNats} from './index.js'

const STATUS_CODE_CANCELED = 2
const STATUS_CODE_VDV_ERROR = 3

const DEBUG = process.env.VDV_453_DEBUG === 'true'

const abortWithError = (err) => {
	console.error(err)
	process.exit(1)
}

const cfg = {}
const subscriptionOpts = {}

const [service] = args
if (typeof service !== 'string' || !service) {
	abortWithError('missing/empty service')
}

if ('leitstelle' in flags) {
	cfg.leitstelle = flags.leitstelle
} else if ('VDV_453_LEITSTELLE' in process.env) {
	cfg.leitstelle = process.env.VDV_453_LEITSTELLE
} else {
	abortWithError('missing/empty --leitstelle/-l/$VDV_453_LEITSTELLE')
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

if ('expires' in flags) {
	if (/^\d+$/.test(flags.expires)) { // UNIX epoch/timestamp
		subscriptionOpts.expires = parseInt(flags.expires)
	} else {
		const expires = DateTime.fromISO(flags.expires, {setZone: true})
		if (expires.zone instanceof SystemZone) {
			abortWithError('--expires ISO 8601 must specify a time zone (offset)')
		}
		subscriptionOpts.expires = expires.toUnixInteger()
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
	const _ = await sendVdv453DataToNats(cfg)
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
