package net.jetztgrad.sesame.redis.util;

import redis.clients.jedis.Jedis;

public class RedisIdGenerator implements IdGenerator {
	private String idName;
	private String runtimeKeyName;
	private Jedis jedis;

	public RedisIdGenerator(String runtimeKeyName, String idName, Jedis jedis) {
		this.runtimeKeyName = runtimeKeyName;
		this.idName = idName;
		this.jedis = jedis;
	}
	
	public String getName() {
		return idName;
	}
	
	@Override
	public long nextId() {
		return jedis.hincrBy(runtimeKeyName, idName, 1);
	}
}
