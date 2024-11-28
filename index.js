import {ok} from 'node:assert'
import {
	Gauge,
	Counter,
	Summary,
} from 'prom-client'
import {
	createClient,
	Vdv453ApiError,
} from 'vdv-453-client'
import {
	createMetricsServer,
	register as metricsRegister,
} from './lib/metrics.js'
import {kBestaetigungZst} from 'vdv-453-client/lib/symbols.js'
import {connectToNats, JSONCodec} from './lib/nats.js'

const sendVdv453DataToNats = async (cfg, opt = {}) => {
	const {
		leitstelle,
		theirLeitstelle,
		endpoint,
		port,
		subscriptions,
	} = cfg
	ok(leitstelle, 'missing/empty cfg.leitstelle')
	ok(theirLeitstelle, 'missing/empty cfg.theirLeitstelle')
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
		checkServerStatusInterval,
		ausManualFetchInterval,
	} = {
		vdv453ClientOpts: {},
		natsOpts: {},
		checkServerStatusInterval: 5 * 1000, // milliseconds
		ausManualFetchInterval: 30 * 1000, // 30 seconds, vdv-453-client's default
		...opt,
	}

	const vdvDatenBereitAnfragesTotal = new Counter({
		name: 'vdv_datenbereitanfrages_total',
		help: 'number of incoming VDV-453 DatenBereitAnfrage requests received from the server',
		registers: [metricsRegister],
		labelNames: [
			'service', // VDV-453/-454 service, e.g. AUS
		],
	})
	const vdvClientStatusAnfragesTotal = new Counter({
		name: 'vdv_clientstatusanfrages_total',
		help: 'number of incoming VDV-453 ClientStatusAnfrage requests received from the server',
		registers: [metricsRegister],
		labelNames: [
			'service', // VDV-453/-454 service, e.g. AUS
		],
	})
	const vdvStatusAntwortsTotal = new Counter({
		name: 'vdv_statusantworts_total',
		help: 'number of VDV-453 StatusAntwort responses from the server',
		registers: [metricsRegister],
		labelNames: [
			'service', // VDV-453/-454 service, e.g. AUS
		],
	})
	const vdvStatusAntwortOkTimestampSeconds = new Gauge({
		name: 'vdv_statusantwort_ok_timestamp_seconds',
		help: 'when the VDV-453 server has last reported as (not) ok via StatusAntwort',
		registers: [metricsRegister],
		labelNames: [
			'service', // VDV-453/-454 service, e.g. AUS
			'status', // 1 = ok, 0 = not ok
		]
	})
	const trackVdvStatusAntwortOk = (svc, isOk, ts = Date.now()) => {
		vdvStatusAntwortOkTimestampSeconds.set({
			service: svc,
			status: isOk ? '1' : '0',
		}, ts / 1000)
	}
	const vdvDatenAbrufenAntwortsTotal = new Counter({
		name: 'vdv_datenabrufenantworts_total',
		help: 'number of VDV-453 DatenAbrufenAntwort responses from the server',
		registers: [metricsRegister],
		labelNames: [
			'service', // VDV-453/-454 service, e.g. AUS
		],
	})

	const activeSubscriptionsTotal = new Gauge({
		name: 'vdv_active_subs_total',
		help: 'number of subscriptions ("Abos")',
		registers: [metricsRegister],
		labelNames: [
			'service', // VDV-453/-454 service, e.g. AUS
		],
	})
	const trackNrOfSubscriptions = (svc, nrOfSubscriptions) => {
		activeSubscriptionsTotal.set({
			service: svc,
		}, nrOfSubscriptions)
	}
	// todo: track subscription events: subscribe, update, expire, cancel

	const dataFetchesTotal = new Counter({
		name: 'vdv_data_fetches_total',
		help: 'number of data fetches (one fetch is a series of DatenAbrufenAnfrage requests)',
		registers: [metricsRegister],
		labelNames: [
			'service', // VDV-453/-454 service, e.g. AUS
			'kind', // started/succeeded/failed
		],
	})
	// todo: track latest successful data fetch?

	const ausIstFahrtsTotal = new Summary({
		name: 'vdv_aus_istfahrts_total',
		help: 'number of VDV-454 AUS IstFahrts obtained in each fetch',
		registers: [metricsRegister],
		labelNames: [
			'datensatz_alle', // if the fetch was with DatensatzAlle=true
		],
	})

	const _updateGaugeWithIso8601Timestamp = (gaugeMetric, iso8601Ts, labels = {}) => {
		const seconds = Date.parse(iso8601Ts) / 1000
		if (Number.isNaN(seconds)) {
			return;
		}
		// todo: find a way that does not not rely on an internal API
		if (seconds >= gaugeMetric._getValue()) {
			gaugeMetric.set(labels, seconds)
		}
	}
	const latestAusIstFahrtZstSeconds = new Gauge({
		name: 'vdv_latest_aus_istfahrt_zst_seconds',
		help: 'latest Zst (timestamp) seen in any VDV-454 AUS IstFahrt',
		registers: [metricsRegister],
	})
	const trackLatestAusIstFahrtZst = (zst) => {
		_updateGaugeWithIso8601Timestamp(latestAusIstFahrtZstSeconds, zst)
	}
	// todo: track our own time, too, in case the two system's date+time diverge?
	const latestServerZstSeconds = new Gauge({
		name: 'vdv_latest_server_zst_seconds',
		help: 'latest Zst (timestamp) seen in any response by the server – effectively a proxy for the server\'s current time',
		registers: [metricsRegister],
	})
	const trackLatestServerZst = (zst) => {
		_updateGaugeWithIso8601Timestamp(latestServerZstSeconds, zst)
	}
	const serverStartDienstZstSeconds = new Gauge({
		name: 'vdv_server_startdienstzst_seconds',
		help: `The server's StatusAntwort.StartDienstZst (timestamp), as obtained from StatusAnfrage requests`,
		registers: [metricsRegister],
		labelNames: [
			'service', // VDV-453/-454 service, e.g. AUS
		],
	})
	const trackServerStartDienstZst = (service, zst) => {
		_updateGaugeWithIso8601Timestamp(serverStartDienstZstSeconds, zst, {service})
	}

	// NATS-related metrics
	// Note: We mirror OpenDataVBB/gtfs-rt-feed's metrics here.
	const natsNrOfMessagesSentTotal = new Counter({
		name: 'nats_nr_of_msgs_sent_total',
		help: 'number of messages sent to NATS',
		registers: [metricsRegister],
		labelNames: [
			'topic_root', // first "segment" of the topic, e.g. `AUS` with `aus.istfahrt.foo.bar`
		],
	})
	const natsLatestMessageSentTimestampSeconds = new Gauge({
		name: 'nats_latest_msg_sent_timestamp_seconds',
		help: 'when the latest message has been sent to NATS',
		registers: [metricsRegister],
		labelNames: [
			'topic_root', // first "segment" of the topic, e.g. `AUS` with `aus.istfahrt.foo.bar`
		],
	})

	const client = createClient({
		...vdv453ClientOpts,
		leitstelle,
		theirLeitstelle,
		endpoint,
	}, {
		// use hooks to expose metrics
		onDatenBereitAnfrage: (svc, datenBereitAnfrage) => {
			vdvDatenBereitAnfragesTotal.inc({
				service: svc,
			})
			if (datenBereitAnfrage.$?.Zst) {
				trackLatestServerZst(datenBereitAnfrage.$?.Zst)
			}
		},
		onClientStatusAnfrage: (svc, clientStatusAnfrage) => {
			vdvClientStatusAnfragesTotal.inc({
				service: svc,
			})
			if (clientStatusAnfrage.$?.Zst) {
				trackLatestServerZst(clientStatusAnfrage.$?.Zst)
			}
		},
		onStatusAntwort: (svc, statusAntwort) => {
			let ts = statusAntwort.Status?.$?.Zst && Date.parse(statusAntwort.Status?.$?.Zst)
			if (!Number.isFinite(ts)) {
				ts = Date.now()
			}
			trackVdvStatusAntwortOk(svc, true, ts)

			vdvStatusAntwortsTotal.inc({
				service: svc,
			})
			if (statusAntwort.Status?.$?.Zst) {
				trackLatestServerZst(statusAntwort.Status?.$?.Zst)
			}
			// todo: track `statusAntwort.DatenVersionID.$text`?
		},
		onSubscribed: (svc, {aboId, aboSubTag, aboSubChildren}, bestaetigung, subStats) => {
			// todo: track even itself?
			// todo: track other parameters?
			trackNrOfSubscriptions(svc, subStats.nrOfSubscriptions)
			if (bestaetigung.$?.Zst) {
				trackLatestServerZst(bestaetigung.$?.Zst)
			}
		},
		onSubscriptionUpdated: (svc, {aboId, aboSubTag, aboSubChildren}, bestaetigung, subStats) => {
			// todo: track even itself?
			// todo: track other parameters?
			trackNrOfSubscriptions(svc, subStats.nrOfSubscriptions)
			if (bestaetigung.$?.Zst) {
				trackLatestServerZst(bestaetigung.$?.Zst)
			}
		},
		onSubscriptionExpired: (svc, {aboId, aboSubTag, aboSubChildren}, subStats) => {
			// todo: track even itself?
			// todo: track other parameters?
			trackNrOfSubscriptions(svc, subStats.nrOfSubscriptions)
		},
		onSubscriptionCanceled: (svc, {aboId, aboSubTag, aboSubChildren}, reason, subStats) => {
			// todo: track even itself?
			// todo: track other parameters?
			trackNrOfSubscriptions(svc, subStats.nrOfSubscriptions)
		},
		onDatenAbrufenAntwort: (svc, {datensatzAlle, weitereDaten, itLevel, bestaetigung}) => {
			vdvDatenAbrufenAntwortsTotal.inc({
				service: svc,
			})
			if (bestaetigung?.$?.Zst) {
				trackLatestServerZst(bestaetigung?.$?.Zst)
			}
			// todo: track datensatzAlle?
			// todo: track weitereDaten?
		},
		onDataFetchStarted: (svc, {datensatzAlle}) => {
			dataFetchesTotal.inc({
				service: svc,
				kind: 'started',
			})
		},
		onDataFetchSucceeded: (svc, {datensatzAlle}, {nrOfFetches, timePassed}) => {
			dataFetchesTotal.inc({
				service: svc,
				kind: 'succeeded',
			})
		},
		onDataFetchFailed: (svc, {datensatzAlle}, err, {nrOfFetches, timePassed}) => {
			dataFetchesTotal.inc({
				service: svc,
				kind: 'failed',
			})
		},
		onAusFetchSucceeded: ({datensatzAlle}, {nrOfIstFahrts}) => {
			ausIstFahrtsTotal.observe({
				datensatz_alle: datensatzAlle,
			}, nrOfIstFahrts)
		},
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
				fetchInterval: ausManualFetchInterval,
			})
			aboId = _aboId

			// todo: process other AUSNachricht children
			client.data.on('aus:IstFahrt', (istFahrt) => {
				if (istFahrt.Zst) { // it seems that not all servers implement this
					trackLatestAusIstFahrtZst(istFahrt.Zst)
				}

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

				// make unenumerable properties regular ones, so that they end up in the JSON
				istFahrt['$BestaetigungZst'] = istFahrt[kBestaetigungZst]

				logger.trace({
					topic,
					istFahrt,
				}, 'publishing AUS IstFahrt to NATS')
				const tSent = Date.now()
				natsClient.publish(topic, natsJson.encode(istFahrt))

				// We slice() to keep the cardinality low in case of a bug.
				const topic_root = (topic.split('.')[0] || '').slice(0, 7)
				natsNrOfMessagesSentTotal.inc({topic_root})
				natsLatestMessageSentTimestampSeconds.set({topic_root}, tSent)
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

	const metricsServer = createMetricsServer()
	await metricsServer.start()
	logger.info(`serving Prometheus metrics on port ${metricsServer.address().port}`)

	const startPromises = []
	const stopTasks = []

	const stop = async () => {
		await Promise.all(stopTasks.map(task => task()))
		client.httpServer.close()
		metricsServer.close()
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

	// fetch server status periodically
	{
		const checkAndResetTimer = async () => {
			const svc = 'aus'

			// setTimeout() handles neither async functions nor rejections, so we must catch errors here by ourselves.
			try {
				const {
					startDienstZst,
				} = await client.ausCheckServerStatus()
				// todo: warn if status is not ok?

				if (startDienstZst === null) {
					logger.warn({
						service: 'aus',
					}, 'server did not provide a StatusAntwort.StartDienstZst')
				} else {
					trackServerStartDienstZst(svc, startDienstZst)
				}
			} catch (err) {
				logger.warn({
					err,
				}, 'failed to check server status')

				// todo: check for the right error message
				if ((err instanceof Vdv453ApiError) && err.statusAntwort) { // server seems not ok
					let ts = err.statusAntwort.Status?.$?.Zst && Date.parse(err.statusAntwort.Status?.$?.Zst)
					if (!Number.isFinite(ts)) {
						ts = Date.now()
					}
					trackVdvStatusAntwortOk(svc, false, ts)
				}
			} finally {
				checkTimer = setTimeout(checkAndResetTimer, checkServerStatusInterval)
			}
		}
		const checkServerStatusInitialWait = Math.max(checkServerStatusInterval / 30, 2_000) // 2 seconds minimum
		let checkTimer = setTimeout(checkAndResetTimer, checkServerStatusInitialWait)

		stopTasks.push(() => {
			clearTimeout(checkTimer)
		})
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
