package net.jetztgrad.sesame.redis;

import net.jetztgrad.sesame.redis.util.IdGenerator;
import net.jetztgrad.sesame.redis.util.KeyBuilder;
import net.jetztgrad.sesame.redis.util.RedisIdGenerator;
import redis.clients.jedis.Jedis;

public abstract class AbstractRedisMappingStrategy implements RedisMappingStrategy, RedisMappingKeys {
	protected final String name;
	protected final String keyPrefix;
	protected RedisValueFactory valueFactory;
	
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
	public RedisValueFactory getValueFactory() {
		if (valueFactory == null) {
			valueFactory = createValueFactory();
		}
		return valueFactory;
	}
	
	protected RedisValueFactory createValueFactory() {
		return new RedisValueFactory();
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
	
	public String createKey(String index) {
		return keyBuilder(index).toString();
	}
}
