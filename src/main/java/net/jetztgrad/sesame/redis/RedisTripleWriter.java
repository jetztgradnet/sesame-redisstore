package net.jetztgrad.sesame.redis;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.sail.SailException;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

public abstract class RedisTripleWriter {
	protected final RedisStoreConnection redisStoreConnection;
	
	public RedisTripleWriter(RedisStoreConnection redisStoreConnection) {
		this.redisStoreConnection = redisStoreConnection;
	}
	
	protected Transaction getActiveTransaction() throws SailException {
		return redisStoreConnection.getActiveTransaction();
	}
	
	protected Jedis getJedisReadClient() {
		return redisStoreConnection.getJedisReadClient();
	}
	
	public abstract void addStatement(Resource subj, URI pred, Value obj,
			Resource[] contexts);

	public abstract void removeStatements(Resource subj, URI pred, Value obj,
			Resource[] contexts);
}
