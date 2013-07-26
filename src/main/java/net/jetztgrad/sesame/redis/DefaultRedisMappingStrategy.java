package net.jetztgrad.sesame.redis;

import net.jetztgrad.sesame.redis.util.IdGenerator;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import redis.clients.jedis.Jedis;

/**
 * This strategy maps quuadruples like this:
 * 
 * <pre>
 * 	subject:(subject) [set/zset]
 * 		(tripleid-1)
 * 		(tripleid-2)
 * 		...
 * 		(tripleid-n)
 * 		
 * 	predicate:(predicate) [set/zset]
 * 		(tripleid-1)
 * 		(tripleid-2)
 * 		...
 * 		(tripleid-n)
 * 	
 * 	object:(object) [set/zset]
 * 		(tripleid-1)
 * 		(tripleid-2)
 * 		...
 * 		(tripleid-n)
 * 	
 * 	context:(ctx) [set/zset]
 * 		(tripleid-1)
 * 		(tripleid-2)
 * 		...
 * 		(tripleid-n)
 * 	
 * 	triple:(tripleid) [hash]
 * 		subject: (subj)
 * 		predicate: (pred)
 * 		object: (val)
 * 		context: (ctx)
 * 	
 * </pre>
 * 
 * @author wschell
 */
public class DefaultRedisMappingStrategy extends AbstractRedisMappingStrategy implements DefaultRedisMappingKeys {
	
	public DefaultRedisMappingStrategy() {
		this(null);
	}
	
	public DefaultRedisMappingStrategy(String keyPrefix) {
		super("default", keyPrefix);
	}
	
	@Override
	public RedisTripleSource createTripleSource(RedisStoreConnection connection, boolean includeInferred) {
		return new DefaultRedisTripleSource(this, connection, includeInferred);
	}

	@Override
	public RedisTripleWriter createTripleWriter(RedisStoreConnection connection) {
		return new DefaultRedisTripleWriter(this, connection);
	}

	public String createSubjectIndex(Resource subject) {
		return keyBuilder(KEY_INDEX_SUBJECTS).k(getValueFactory().encode(subject)).toString();
	}
	
	public String createPredicateIndex(URI predicate) {
		return keyBuilder(KEY_INDEX_PREDICATES).k(getValueFactory().encode(predicate)).toString();
	}
	
	public String createObjectIndex(Value object) {
		return keyBuilder(KEY_INDEX_OBJECTS).k(getValueFactory().encode(object)).toString();
	}
	
	public String createContextIndex(Resource context) {
		return keyBuilder(KEY_INDEX_CONTEXTS).k(getValueFactory().encode(context)).toString();
	}
	
	public String createStatementKey(Resource subj, Jedis jedis) {
		IdGenerator idGenerator = getIdGenerator(ID_STATEMENT, jedis);
		long id = idGenerator.nextId();
		return keyBuilder(KEY_STATEMENT).k(Long.toString(id)).toString();
	}
}
