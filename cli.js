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
	},
	allowPositionals: true,
})

if (flags.help) {
	process.stdout.write(`
Usage:
    send-vdv-453-data-to-nats [options]
Examples:
    send-vdv-453-data-to-nats
\n`)
	process.exit(0)
}

if (flags.version) {
	process.stdout.write(`${pkg.name} v${pkg.version}\n`)
	process.exit(0)
}

// todo
