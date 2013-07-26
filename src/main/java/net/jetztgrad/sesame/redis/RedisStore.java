package net.jetztgrad.sesame.redis;

import org.apache.commons.pool.impl.GenericObjectPool.Config;
import org.openrdf.model.Resource;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.sail.NotifyingSailConnection;
import org.openrdf.sail.SailException;
import org.openrdf.sail.helpers.NotifyingSailBase;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisException;

/**
 * TripleStore based on Redis.
 * 
 * @author wschell
 */
public class RedisStore extends NotifyingSailBase {

	protected RedisValueFactory valueFactory;
	protected JedisPool pool;
	protected Resource defaultContext;
	protected RedisMappingStrategy redisMapping;
	
	public RedisStore() {
	}
	
	@Override
	protected void initializeInternal() throws SailException {
		super.initializeInternal();
		
		// TODO make configurable
		String host = "localhost";
		int port = Protocol.DEFAULT_PORT;
		int timeout = Protocol.DEFAULT_TIMEOUT;
		String password = null;
		int database = Protocol.DEFAULT_DATABASE;
		Config config = new JedisPoolConfig();
		
		this.pool = new JedisPool(config, host, port, timeout, password, database);
		
		try {
			testConnectionInternal();
		}
		catch (JedisException e) {
			throw new SailException("failed to connect to Redis database: " + e.getMessage(), e);
		}
		
		// TODO autodetect config incl. used mapping from Redis store
		String mappingStrategy = null;
		this.redisMapping = new RedisMappingFactoryImpl().getMapping(mappingStrategy);
		this.valueFactory = this.redisMapping.getValueFactory();
		this.defaultContext = new URIImpl("urn:default");
	}
	
	public void setDefaultContext(Resource defaultContext) {
		this.defaultContext = defaultContext;
	}
	
	public void setDefaultContext(String defaultContext) {
		this.defaultContext = new URIImpl(defaultContext);
	}
	
	public Resource getDefaultContext() {
		return defaultContext;
	}
	
	public boolean testConnection() {
		try {
			return testConnectionInternal();
		}
		catch (JedisException e) {
			// ignore
			return false;
		}
	}
	
	protected boolean testConnectionInternal() throws JedisException {
		// test connection
		Jedis jedis = pool.getResource();
		try {
			jedis.connect();
			jedis.ping();
			return true;
		}
		finally {
			pool.returnResource(jedis);
		}
	}

	protected Jedis getJedisClient() {
		Jedis jedis = pool.getResource();
		return jedis;
	}
	
	protected void releaseJedisClient(Jedis jedis) {
		pool.returnResource(jedis);
	}
	
	@Override
	public RedisStoreConnection getConnection() throws SailException {
		return (RedisStoreConnection) super.getConnection();
	}

	@Override
	public boolean isWritable() throws SailException {
		return true;
	}

	@Override
	public RedisValueFactory getValueFactory() {
		return valueFactory;
	}

	@Override
	protected NotifyingSailConnection getConnectionInternal()
			throws SailException {
		return new RedisStoreConnection(this);
	}

	@Override
	protected void shutDownInternal() throws SailException {
		pool.destroy();
	}
	
	public RedisMappingStrategy getMappingStrategy() {
		return redisMapping;
	}
}
