package net.jetztgrad.sesame.redis;

import net.jetztgrad.sesame.redis.util.KeyBuilder;

import org.openrdf.model.ValueFactory;
import org.openrdf.query.algebra.evaluation.TripleSource;

/**
 * The mapping strategy specifies how to map triples or quadruples to Redis keys and data structures.
 *  
 * @author wschell
 */
public interface RedisMappingStrategy {
	/**
	 * Get name for this mapping. 
	 * @return mapping name
	 */
	String getMappingName();
	
	/**
	 * Get key builder for specified key name. This allows e.g. to prefix all keys 
	 * so as to have multiple triple stores within a single Redis instance.
	 * 
	 * @param key (start of) the key
	 * 
	 * @return fully qualified key string
	 */
	String key(String key);
	
	
	/**
	 * Get key builder for specified key name. This allows e.g. to prefix all keys 
	 * so as to have multiple triple stores within a single Redis instance.
	 * 
	 * @param key (start of) the key
	 * 
	 * @return a KeyBuilder, which can be used to append additional key parts
	 */
	KeyBuilder keyBuilder(String key);
	
	/**
	 * Create a {@link TripleSource} for this mapping strategy.
	 * 
	 * @param connection connection to the Redis store
	 * @param includeInferred Indicates whether inferred triples are to be considered in the
	 *        query result. If false, no inferred statements are returned; if
	 *        true, inferred statements are returned if available
	 * 
	 * @return triple source
	 */
	RedisTripleSource createTripleSource(RedisStoreConnection connection, boolean includeInferred);
	
	/**
	 * Create a {@link RedisTripleWriter} for this mapping strategy.
	 * 
	 * @param connection connection to the Redis store
	 * 
	 * @return triple writer
	 */
	RedisTripleWriter createTripleWriter(RedisStoreConnection connection);
	
	
	/**
	 * Get the {@link ValueFactory} for this mapping strategy.
	 * 
	 * @return value factory
	 */
	RedisValueFactory getValueFactory();

	/**
	 * Get key of config hash. The config hash contains settings for configuration of this triple store,
	 * the values are mostly static.
	 *  
	 * @return key of config hash
	 */
	String configKey();
	
	/**
	 * Get key of runtime hash. The config hash contains runtime data of this triple store,
	 * the values can be dynamic. An example could be the next id to be used for an object.
	 * 
	 * @return key of runtime hash
	 */
	String runtimeKey();
	
}
