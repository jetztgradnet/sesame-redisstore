package net.jetztgrad.sesame.redis;

import redis.clients.jedis.Jedis;
import net.jetztgrad.sesame.redis.util.IdGenerator;
import net.jetztgrad.sesame.redis.util.KeyBuilder;
import net.jetztgrad.sesame.redis.util.RedisIdGenerator;

public abstract class AbstractRedisMappingStrategy implements RedisMappingStrategy {
	public final static String HKEY_SYSTEM_NEXT_ID = "nextId";
	
	public final static String KEY_CONFIG = "config";
	public final static String KEY_RUNTIME = "runtime";
	
	protected final String name;
	protected final String keyPrefix;
	
	public AbstractRedisMappingStrategy(String name) {
		this(name, null);
	}

	public AbstractRedisMappingStrategy(String name, String keyPrefix) {
		this.name = name;
		this.keyPrefix = keyPrefix;
	}

	@Override
	public String getMappingName() {
		return name;
	}
	
	@Override
	public KeyBuilder keyBuilder(String key) {
		return newKeyBuilder(keyPrefix).appendKey(key);
	}
	
	@Override
	public String key(String key) {
		return keyBuilder(key).toString();
	}

	@Override
	public String configKey() {
		return key(KEY_CONFIG);
	}
	
	@Override
	public String runtimeKey() {
		return key(KEY_CONFIG);
	}
	
	protected KeyBuilder newKeyBuilder(String keyPrefix) {
		return new KeyBuilder(keyPrefix);
	}
	
	protected IdGenerator getIdGenerator(String idName, Jedis jedis) {
		return new RedisIdGenerator(runtimeKey(), idName, jedis);
	}
}
