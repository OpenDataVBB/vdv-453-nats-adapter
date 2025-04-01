import {Redis} from 'ioredis'

// compatible with vdv-453-client's `openStorage` interface
const openRedisStorage = async () => {
	const redis = new Redis(process.env.REDIS_URL || {})
	await redis.ping() // make sure the connection works

	const has = async (key) => {
		return (await redis.exists(key)) === 1
	}

	const get = async (key) => {
		return await redis.get(key)
	}

	const set = async (key, val, expiresInMs = Infinity) => {
		const expireArgs = expiresInMs !== Infinity ? ['PX', String(expiresInMs)] : []
		await redis.set(key, val, ...expireArgs)
	}

	const del = async (key) => {
		await redis.del(key)
	}

	const keys = async (prefix = null) => {
		const pattern = prefix + '*'
		let _keys = []
		let cursor = '0'
		while (true) {
			const [newCursor, newkeys] = await redis.scan(cursor, 'MATCH', pattern)
			_keys = _keys.concat(newkeys)
			if (newCursor === '0') break
			cursor = newCursor
		}
		return _keys
	}

	const entries = async (prefix = null) => {
		const _keys = await keys(prefix)
		// todo: this doesn't scale, do it iteratively
		const _values = _keys.length > 0
			? await redis.mget(..._keys)
			: []

		const _entries = new Array(_keys.length)
		for (let i = 0; i < _keys.length; i++) {
			_entries[i] = [_keys[i], _values[i]]
		}
		return _entries
	}

	return {
		has,
		get,
		set,
		del,
		keys,
		entries,
	}
}

export {
	openRedisStorage,
}
