import {ok} from 'node:assert'
import {createClient} from 'vdv-453-client'

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
	} = {
		vdv453ClientOpts: {},
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

	const subscribeToAUS = () => {
		let aboId = null
		const startTask = async () => {
			const {aboId: _aboId} = await client.ausSubscribe({
				expiresAt: Date.now() + 10 * 60 * 1000, // 10m from now, vdv-453-client uses milliseconds
			})
			aboId = _aboId

			// todo: process other AUSNachricht children
			client.data.on('aus:IstFahrt', (istFahrt) => {
				// todo
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
