package net.jetztgrad.sesame.redis;

import net.jetztgrad.sesame.redis.util.KeyBuilder;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;

public class DefaultRedisMappingStrategy extends AbstractRedisMappingStrategy {
	public DefaultRedisMappingStrategy() {
		super("default");
	}
	
	public String getKeyFor(Resource resource) {
		if (resource instanceof URI) {
			URI uri = (URI) resource;
			// TODO do we need to encode some part?
			return uri.stringValue();
			// prefix with SUBJECT
			//return concat(SUBJECT, uri.stringValue());
		}
		// TODO how to handle BNodes?
		return null;
	}

	@Override
	protected KeyBuilder newKeyBuilder(String keyPrefix) {
		return new KeyBuilder(keyPrefix);
	}

	@Override
	public RedisTripleSource createTripleSource(RedisStoreConnection connection, boolean includeInferred) {
		return new DefaultRedisTripleSource(connection, includeInferred);
	}

	@Override
	public RedisTripleWriter createTripleWriter(RedisStoreConnection connection) {
		return new DefaultRedisTripleWriter(connection);
	}
}
