package net.jetztgrad.sesame.redis;

import info.aduna.iteration.CloseableIteration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.evaluation.iterator.CollectionIteration;

import redis.clients.jedis.Jedis;

public class DefaultRedisTripleSource extends RedisTripleSource implements DefaultRedisMappingKeys {

	protected final DefaultRedisMappingStrategy mappingStrategy;

	public DefaultRedisTripleSource(DefaultRedisMappingStrategy mappingStrategy, RedisStoreConnection connection, boolean includeInferred) {
		super(connection, includeInferred);
		this.mappingStrategy = mappingStrategy;
	}
	
	public CloseableIteration<? extends Statement, QueryEvaluationException> getStatements(
			Resource subj, URI pred, Value obj, Resource... contexts)
			throws QueryEvaluationException {
		Map<Resource, Set<String>> statementsByContext = findStatements(subj, pred, obj, contexts);
		Collection<Statement> statements = new ArrayList<Statement>();
		for (Entry<Resource, Set<String>> entry : statementsByContext.entrySet()) {
			Resource context = entry.getKey();
			boolean isDefaultContext = getConnection().isDefaultContext(context);
			Set<String> statementKeys = entry.getValue();
			for (String statementKey : statementKeys) {
				// no context
				Statement statement = readStatement(statementKey, (isDefaultContext ? null : context));
				if (statement != null) {
					statements.add(statement);
				}
			}
		}
		
		return new CollectionIteration<Statement, QueryEvaluationException>(statements);
	}
	
	public Map<Resource, Set<String>> findStatements(Resource subj, URI pred, Value obj, Resource... contexts) {
		contexts = getConnection().checkContexts(contexts);
		List<String> indexKeys = new ArrayList<String>();
		if (subj != null) {
			String subjectIndex = mappingStrategy.createSubjectIndex(subj);
			indexKeys.add(subjectIndex);
		}
		if (pred != null) {
			String predicateIndex = mappingStrategy.createPredicateIndex(pred);
			indexKeys.add(predicateIndex);
		}
		if (obj != null) {
			String objectIndex = mappingStrategy.createObjectIndex(obj);
			indexKeys.add(objectIndex);
		}
		
		// TODO remove me?
		if (indexKeys.isEmpty()) {
//			indexKeys.add(mappingStrategy.createKey(KEY_STATEMENTS));
		}
		
		Jedis client = getReadClient();

		Map<Resource, Set<String>> statementKeys = new HashMap<Resource, Set<String>>();
		for (Resource context : contexts) {
			Set<String> keys = findStatements(client, indexKeys, context);
			if (!keys.isEmpty()) {
				statementKeys.put(context, keys);
			}
		}
		
		return statementKeys;
	}
	
	protected Set<String> findStatements(Jedis client, List<String> subjectPredicateObjectIndexKeys, Resource context) {
		String contextIndex = mappingStrategy.createContextIndex(context);
		
		List<String> indexKeysWithContext = subjectPredicateObjectIndexKeys;
		if (context != null) {
			indexKeysWithContext = new ArrayList<String>(subjectPredicateObjectIndexKeys);
			indexKeysWithContext.add(contextIndex);
		}
		
		if (indexKeysWithContext.isEmpty()) {
			return Collections.emptySet();
		}
		
		Set<String> keys = client.sinter((String[]) indexKeysWithContext
				.toArray(new String[indexKeysWithContext.size()]));
		
		return keys;
	}
	
	public Statement readStatement(String statementKey, Resource context) {
		Jedis client = getReadClient();
		RedisValueFactory factory = getValueFactory();
		
		Map<String, String> statementData = client.hgetAll(statementKey);
		if ((statementData == null) || statementData.isEmpty()) {
			return null;
		}
		Resource subject = factory.createBNodeOrURI(statementData.get(HKEY_SUBJECT));
		URI predicate = factory.createURI(statementData.get(HKEY_PREDICATE));
		String type = statementData.get(HKEY_OBJECT_DATATYPE);
		String language = statementData.get(HKEY_OBJECT_LANGUAGE);
		Value object = factory.createValue(statementData.get(HKEY_OBJECT), type, language);
//		Resource context = factory.createURI(statementData.get(HKEY_CONTEXT));
		if (context != null) {
			return factory.createStatement(subject, predicate, object, context);
		}
		
		return factory.createStatement(subject, predicate, object);
	}
	
	@Override
	public long size(Resource[] contexts) {
		long size = 0;
		
		// method 1 is slower but correct
		// method 2 is faster but does not respect statements in multiple contexts
		boolean useFindStatements = true;
		
		if (useFindStatements) {
			// method 1: find all matching statement ids and count them
			Map<Resource, Set<String>> statementsByContext = findStatements(null, null, null, contexts);
			Set<String> statements = new HashSet<String>();
			for (Set<String> st : statementsByContext.values()) {
				statements.addAll(st);
			}
			return statements.size();
		}
		else {
			// method 2: sum up size of context index
			// TODO this method does not respect statements in multiple contexts
			Jedis client = getReadClient();
			
			contexts = getConnection().checkContexts(contexts);
			for (Resource ctx : contexts) {
				String contextIndex = mappingStrategy.createContextIndex(ctx);
				size += client.scard(contextIndex);
			}
			
			return size;
		}
	}
}
