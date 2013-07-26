package net.jetztgrad.sesame.redis;

public interface DefaultRedisMappingKeys extends RedisMappingKeys {
	public static final String ID_STATEMENT = "statementId";
	
	public static final String KEY_STATEMENTS = "statements";
	public static final String KEY_STATEMENT = "statement";
	public static final String KEY_INDEX_SUBJECTS = "index:subject";
	public static final String KEY_INDEX_PREDICATES = "index:predicate";
	public static final String KEY_INDEX_OBJECTS = "index:object";
	public static final String KEY_INDEX_CONTEXTS = "index:context";
	
	
	public static final String HKEY_SUBJECT = "subject";
	public static final String HKEY_PREDICATE = "predicate";
	public static final String HKEY_OBJECT = "object";
	public static final String HKEY_OBJECT_DATATYPE = "datatype";
	public static final String HKEY_OBJECT_LANGUAGE = "language";
	public static final String HKEY_CONTEXT = "context";
}
