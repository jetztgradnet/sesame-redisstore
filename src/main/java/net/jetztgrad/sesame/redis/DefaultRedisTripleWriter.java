package net.jetztgrad.sesame.redis;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

public class DefaultRedisTripleWriter extends RedisTripleWriter {

	public DefaultRedisTripleWriter(RedisStoreConnection redisStoreConnection) {
		super(redisStoreConnection);
	}

	@Override
	public void addStatement(Resource subj, URI pred, Value obj,
			Resource[] contexts) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void removeStatements(Resource subj, URI pred, Value obj,
			Resource[] contexts) {
		// TODO Auto-generated method stub
		
	}

}
