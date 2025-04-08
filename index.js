import {createHash} from 'node:crypto'
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
import {openRedisStorage} from './lib/redis-store.js'
import {
	createMetricsServer,
	register as metricsRegister,
} from './lib/metrics.js'
import {SERVICES} from 'vdv-453-client/lib/services.js'
import {kBestaetigungZst} from 'vdv-453-client/lib/symbols.js'
import {connectToNats, JSONCodec} from './lib/nats.js'

const _noop = () => {}

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
		ausCheckServerStatusInterval,
		ausManualFetchInterval,
	} = {
		vdv453ClientOpts: {},
		natsOpts: {},
		ausCheckServerStatusInterval: 5 * 1000, // 5 seconds
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
	// todo: switch back to a regular Gauge but set the measurement timestamp manually, once that's possible again?
	// see also https://pkg.go.dev/github.com/prometheus/client_golang/prometheus#NewMetricWithTimestamp
	// see also https://github.com/siimon/prom-client/issues/177
	// see also https://github.com/siimon/prom-client/issues/590
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
		help: `The server's StatusAntwort.StartDienstZst (timestamp), as obtained from StatusAntwort responses`,
		registers: [metricsRegister],
		labelNames: [
			'service', // VDV-453/-454 service, e.g. AUS
		],
	})
	const trackServerStartDienstZst = (service, zst) => {
		_updateGaugeWithIso8601Timestamp(serverStartDienstZstSeconds, zst, {service})
	}
	const serverDatenVersionIDSeconds = new Gauge({
		name: 'vdv_server_datenversionid',
		help: `The server's StatusAntwort.DatenVersionID, as obtained from StatusAntwort responses. A timestamp if it is in ISO 8601 format, otherwise a hash.`,
		registers: [metricsRegister],
		labelNames: [
			'service', // VDV-453/-454 service, e.g. AUS
		],
	})
	const trackServerDatenVersionID = (service, datenVersionID) => {
		const parsedAsTimestamp = Date.parse(datenVersionID)
		const mappedDatenVersionID = Number.isInteger(parsedAsTimestamp)
			? parsedAsTimestamp / 1000
			: createHash('sha1').update(datenVersionID).digest().readUInt32LE() // 4 bytes should be enough
		serverDatenVersionIDSeconds.set({service}, mappedDatenVersionID)
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

	const activeSubscriptions = []

	const client = await createClient({
		...vdv453ClientOpts,
		leitstelle,
		theirLeitstelle,
		endpoint,
	}, {
		openStorage: openRedisStorage,
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
		},
		onSubscriptionCreated: (svc, {aboId, expiresAt, aboSubTag, aboSubChildren}, bestaetigung, subStats) => {
			// todo: track even itself?
			// todo: track other parameters?
			trackNrOfSubscriptions(svc, subStats.nrOfSubscriptions)
			if (bestaetigung.$?.Zst) {
				trackLatestServerZst(bestaetigung.$?.Zst)
			}
		},
		onSubscriptionRestored: (service, {aboId, expiresAt}) => {
			// todo: check if already exists?
			activeSubscriptions.push({service, expiresAt, aboId})
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
		onSubscriptionsResetByServer: async (svc, subsStats) => {
			// todo: debug-log?
			await subscribe()
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

	const publishToNats = (topic, item) => {
		// todo: trace-log?
		const tSent = Date.now()
		natsClient.publish(topic, natsJson.encode(item))

		// We slice() to keep the cardinality low in case of a bug.
		const topic_root = (topic.split('.')[0] || '').slice(0, 7)
		natsNrOfMessagesSentTotal.inc({topic_root})
		natsLatestMessageSentTimestampSeconds.set({topic_root}, tSent)
	}

	{
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

			publishToNats(topic, istFahrt)
		})
	}

	const _subscribe = (subscribeMethod, unsubscribeMethod, ...subscribeArgs) => {
		let aboId = null
		// todo: support `expires` value of `'never'`/`Infinity`, re-subscribing continuously?
		const startTask = async () => {
			const {aboId: _aboId} = await client[subscribeMethod](...subscribeArgs)
			aboId = _aboId
		}
		const unsubscribe = async () => {
			if (aboId === null) return;
			await client[unsubscribeMethod](aboId)
		}

		const startPromise = startTask()

		return {
			startPromise,
			stopTask: unsubscribe,
		}
	}

	const metricsServer = createMetricsServer()
	await metricsServer.start()
	logger.info(`serving Prometheus metrics on port ${metricsServer.address().port}`)

	const stopTasks = []

	const stop = async () => {
		await Promise.all(stopTasks.map(task => task()))
		client.httpServer.close()
		metricsServer.close()
	}

	const startCheckingServerStatusPeriodically = (svc, checkMethod, interval, processServerStatus = _noop) => {
		const logCtx = {
			service: svc,
		}

		const checkAndResetTimer = async () => {
			// setTimeout() handles neither async functions nor rejections, so we must catch errors here by ourselves.
			try {
				const serverStatus = await client[checkMethod]()
				await processServerStatus()

				const {
					startDienstZst,
					datenVersionID,
				} = serverStatus
				// todo: warn if status is not ok?

				if (startDienstZst === null) {
					logger.warn({
						...logCtx,
					}, 'server did not provide a StatusAntwort.StartDienstZst')
				} else {
					trackServerStartDienstZst(svc, startDienstZst)
				}
				if (datenVersionID === null) {
					logger.warn({
						...logCtx,
					}, 'server did not provide a StatusAntwort.DatenVersionID')
				} else {
					trackServerDatenVersionID(svc, datenVersionID)
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

				// todo: throw programmer errors!
			} finally {
				checkTimer = setTimeout(checkAndResetTimer, interval)
			}
		}
		const initialWait = Math.max(interval / 30, 2_000) // 2 seconds minimum
		let checkTimer = setTimeout(checkAndResetTimer, initialWait)

		stopTasks.push(() => {
			clearTimeout(checkTimer)
		})
	}

	startCheckingServerStatusPeriodically(
		SERVICES.AUS,
		'ausCheckServerStatus',
		ausCheckServerStatusInterval,
	)

	// (re-)create all subscriptions specified by `subscriptions`
	const subscribe = async () => {
		logger.info({
			subscriptions,
		}, '(re)creating all subscriptions')

		const subscribeFns = subscriptions
		.map((subscription) => {
			const {
				service,
				expires,
			} = subscription

			if (activeSubscriptions.find((sub) => sub.service === service && sub.expiresAt === expires)) {
				// todo: info-log
				return null
			}

			let subscribe = null
			if (service === SERVICES.AUS) {
				subscribe = () => {
					return _subscribe('ausSubscribe', 'ausUnsubscribe', {
						expiresAt: expires * 1000, // vdv-453-client uses milliseconds
						fetchInterval: ausManualFetchInterval,
					})
				}
			} else {
				throw new Error(`invalid/unsupported service "${service}"`)
			}

			return async () => {
				const {
					startPromise,
					stopTask,
				} = subscribe()
				stopTasks.push(stopTask)
				return await startPromise
			}
		})
		.filter(subscribe => subscribe !== null)

		// todo: cancel successfully created subscriptions & exit if one of these fails
		await Promise.all(
			subscribeFns.map(subscribe => subscribe())
		)
	}

	await subscribe()

	return {
		logger,
		client,
		stop,
	}
}

export {
	SERVICES,
	sendVdv453DataToNats,
}
