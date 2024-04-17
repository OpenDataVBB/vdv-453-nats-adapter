import {ok} from 'node:assert'
import {createClient} from 'vdv-453-client'
import {connectToNats, JSONCodec} from './lib/nats.js'

const sendVdv453DataToNats = async (cfg, opt = {}) => {
	const {
		leitstelle,
		endpoint,
		port,
		subscriptions,
	} = cfg
	ok(leitstelle, 'missing/empty cfg.leitstelle')
	ok(endpoint, 'missing/empty cfg.endpoint')
	ok(Number.isInteger(port), 'cfg.port must be an integer')
	ok(Array.isArray(subscriptions), 'cfg.subscriptions must be an array')
	ok(subscriptions.length > 0, 'cfg.subscriptions must not be empty')
	for (let i = 0; i < subscriptions.length; i++) {
		const {
			service,
			expires,
		} = subscriptions[i]
		ok(service, `invalid/empty cfg.subscriptions[${i}].service`)
		// todo: handle BigInt?
		ok(Number.isInteger(expires), `cfg.subscriptions[${i}].expires must be a UNIX epoch/timestamp`)
	}

	const {
		vdv453ClientOpts,
		natsOpts,
	} = {
		vdv453ClientOpts: {},
		natsOpts: {},
		...opt,
	}

	const client = createClient({
		...vdv453ClientOpts,
		leitstelle,
		endpoint,
	})
	const {
		logger,
	} = client

	await new Promise((resolve, reject) => {
		client.httpServer.listen(port, (err) => {
			if (err) reject(err)
			else resolve()
		})
	})
	logger.info(`listening on port ${port}`)

	const {
		natsClient,
	} = await connectToNats({
		logger,
	}, natsOpts)
	const natsJson = JSONCodec()
	// todo: warn-log publish failures?

	const subscribeToAUS = (expires) => {
		let aboId = null
		// todo: support `expires` value of `'never'`/`Infinity`, re-subscribing continuously?
		const startTask = async () => {
			const {aboId: _aboId} = await client.ausSubscribe({
				expiresAt: expires * 1000, // vdv-453-client uses milliseconds
			})
			aboId = _aboId

			// todo: process other AUSNachricht children
			client.data.on('aus:IstFahrt', (istFahrt) => {
				const emptySegment = '_'
				// > Recommended characters: `a` to `z`, `A` to `Z` and `0` to `9` (names […] cannot contain whitespace).
				// > Special characters: The period `.` and `*` and also `>`.
				// Note: By mapping IDs with non-recommended characters to `_`, we accept a low chance of ID collisions here, e.g. between `foo.bar >baz` and `foo_bar__baz`.
				// todo: consider replacing only special/unsafe characters (`.`/`*`/`>`/` `)
				const escapeTopicSegment = id => id.replace(/[^a-zA-Z0-9]/g, '_')

				const {
					LinienID: linienId,
					LinienText: linienText,
					RichtungsID: richtungsId,
					RichtungsText: richtungsText,
				} = istFahrt
				const {
					FahrtBezeichner: fahrtBezeichner,
					Betriebstag: betriebstag,
				} = istFahrt.FahrtID || {}

				// We make up a hierarchical topic `aus.istfahrt.$linie.$richtung.$fahrt` that allows consumers to pre-filter.
				// With some IstFahrts some IDs are missing, so we use the test equivalents as fallbacks.
				const linieSegment = linienId
					? `id:${escapeTopicSegment(linienId)}`
					: (linienText
						// todo: add configurable text normalization? e.g. Unicode -> ASCII, lower case
						? `text:${escapeTopicSegment(linienText)}`
						: emptySegment
					)
				const richtungSegment = richtungsId
					? `id:${escapeTopicSegment(richtungsId)}`
					: (richtungsText
						// todo: add configurable text normalization? e.g. Unicode -> ASCII, lower case
						? `text:${escapeTopicSegment(richtungsText)}`
						: emptySegment
					)
				const fahrtSegment = fahrtBezeichner && betriebstag
					? `id:${escapeTopicSegment(fahrtBezeichner)}:tag:${escapeTopicSegment(betriebstag)}`
					: emptySegment
				const topic = `aus.istfahrt.${linieSegment}.${richtungSegment}.${fahrtSegment}`

				logger.trace({
					topic,
					istFahrt,
				}, 'publishing AUS IstFahrt to NATS')
				natsClient.publish(topic, natsJson.encode(istFahrt))
			})
		}

		const unsubscribeFromAUS = async () => {
			if (aboId === null) return;
			await client.ausUnsubscribe(aboId)
		}

		const startPromise = startTask()

		return {
			startPromise,
			stopTask: unsubscribeFromAUS,
		}
	}

	const startPromises = []
	const stopTasks = []

	const stop = async () => {
		await Promise.all(stopTasks.map(task => task()))
		client.httpServer.close()
	}

	for (const subscription of subscriptions) {
		const {
			service,
			expires,
		} = subscription
		if (subscription.service === 'AUS') {
			const {
				startPromise,
				stopTask,
			} = subscribeToAUS(expires)
			startPromises.push(startPromise)
			stopTasks.push(stopTask)
		} else {
			throw new Error(`invalid/unsupported service "${service}"`)
		}
	}

	// todo: stop successfully started subscriptions if one of these fails
	await Promise.all(startPromises)

	return {
		logger,
		client,
		stop,
	}
}

export {
	sendVdv453DataToNats,
}
