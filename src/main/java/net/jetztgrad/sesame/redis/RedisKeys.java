package net.jetztgrad.sesame.redis;

public interface RedisKeys {
	public final static String CONTEXT_DEFAULT = "__default__";
	public final static String CONTEXT_ALL = "__all__";
	
	public final static String NAMESPACES = "namespaces";
	public final static String CONTEXTS = "contexts";
	public final static String CONTEXTS_STATEMENT_SIZE = "contexts-size";
	
	public final static String CONTEXT = "context";
	public final static String SUBJECT = "subject";
	public final static String PREDICATE = "predicate";
}
