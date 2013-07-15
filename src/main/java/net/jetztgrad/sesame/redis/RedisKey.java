package net.jetztgrad.sesame.redis;

public enum RedisKey {
	CONTEXTS("contexts", true),
	NAMESPACES("namespaces", false),
	
	CONTEXT("context:", true),
	SUBJECT("subject:", true),
	PREDICATE("predicate:", true);
	
	private String keyName;
	private boolean prefix;
	
	RedisKey(String keyName, boolean prefix) {
		this.keyName = keyName;
		this.prefix = prefix;
	}
	
	public String keyName() {
		return keyName;
	}
	
	public boolean prefix() {
		return prefix;
	}
	
	@Override
	public String toString() {
		return keyName;
	}
}
