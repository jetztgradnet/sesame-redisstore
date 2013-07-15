package net.jetztgrad.sesame.redis;

import org.openrdf.model.Resource;

public interface RedisMapping {
	/**
	 * Get key name for specified symbolic name. This allows e.g. to prefix all system keys 
	 * so as to have multiple triple stores within a single Redis instance.
	 * 
	 * @param name symbolic name of field to return
	 * 
	 * @return key for name
	 */
	public String getSystemKey(RedisKey name);
	
	public String getSystemKey(RedisKey prefix, String name);
	
	public String getKeyFor(Resource resource);
}
