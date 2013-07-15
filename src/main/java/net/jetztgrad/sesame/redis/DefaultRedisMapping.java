package net.jetztgrad.sesame.redis;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;

import static net.jetztgrad.sesame.redis.RedisKey.*;

public class DefaultRedisMapping implements RedisMapping {
	public final static char DEFAULT_KEYSEPARATOR = ':';
	protected final String prefix;
	protected final char keySeparator;

	public DefaultRedisMapping() {
		this(null, DEFAULT_KEYSEPARATOR);
	}
	
	public DefaultRedisMapping(final String prefix) {
		this(prefix, DEFAULT_KEYSEPARATOR);
	}

	public DefaultRedisMapping(final char keySeparator) {
		this(null, keySeparator);
	}

	public DefaultRedisMapping(final String prefix, final char keySeparator) {
		this.prefix = prefix;
		this.keySeparator = keySeparator;
	}
	
	public String getPrefix() {
		return prefix;
	}
	
	public String getSystemKey(RedisKey name) {
		return name.keyName();
	}
	
	public String getSystemKey(RedisKey prefix, String name) {
		if (!prefix.prefix()) {
			throw new IllegalArgumentException(prefix.name() + " is not a prefix");
		}
		return prefix.keyName() + name;
	}
	
	protected String concat(String... parts) {
		if (parts == null) return null;
		if (parts.length == 0) return null;
		if (parts.length == 1) return parts[0];
		
		StringBuilder b = new StringBuilder();
		for (int o = 0; o < parts.length; o++) {
			String part = parts[o];
			if (b.length() > 0 
				&& (b.charAt(b.length()) != keySeparator)) {
				b.append(keySeparator);
			}
			b.append(part);
		}
		return b.toString();
	}

	@Override
	public String getKeyFor(Resource resource) {
		if (resource instanceof URI) {
			URI uri = (URI) resource;
			// prefix with SUBJECT
			return SUBJECT.keyName() + uri.stringValue();
		}
		// TODO Auto-generated method stub
		return null;
	}

}
