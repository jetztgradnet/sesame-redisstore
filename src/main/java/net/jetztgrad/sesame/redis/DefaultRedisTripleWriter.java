package net.jetztgrad.sesame.redis;

import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.sail.SailException;

import redis.clients.jedis.Transaction;

public class DefaultRedisTripleWriter extends RedisTripleWriter implements DefaultRedisMappingKeys {

	protected final DefaultRedisMappingStrategy mappingStrategy;

	public DefaultRedisTripleWriter(DefaultRedisMappingStrategy mappingStrategy, RedisStoreConnection connection) {
		super(connection);
		this.mappingStrategy = mappingStrategy;
	}

	@Override
	public void addStatement(Resource subj, URI pred, Value obj,
			Resource[] contexts) throws SailException {
		String statementKey = null;
		contexts = getConnection().checkContexts(contexts);

		RedisValueFactory factory = getConnection().getValueFactory();
		Transaction transaction = getActiveTransaction();
		
		// find existing statements and overwrite them
		DefaultRedisTripleSource tripleSource = new DefaultRedisTripleSource(mappingStrategy, getConnection(), false);
		Map<Resource, Set<String>> statementsByContext = tripleSource.findStatements(subj, pred, obj, contexts);
		if ((statementsByContext != null) && !statementsByContext.isEmpty()) {
			// get first statement key of first context
			for (Set<String> statementKeys : statementsByContext.values()) {
				if (!statementKeys.isEmpty()) {
					statementKey = statementKeys.iterator().next();
					break;
				}
			}
		}
		// TODO also check local transaction cache of added statements
		else {
			// new statement, create key
			statementKey = mappingStrategy.createStatementKey(subj, getReadClient());
			String statementIndex = mappingStrategy.createKey(KEY_STATEMENTS);
			String subjectIndex = mappingStrategy.createSubjectIndex(subj);
			String predicateIndex = mappingStrategy.createPredicateIndex(pred);
			String objectIndex = mappingStrategy.createObjectIndex(obj);
			
			// write statement hash
			transaction.hset(statementKey, HKEY_SUBJECT, factory.encode(subj));
			transaction.hset(statementKey, HKEY_PREDICATE, factory.encode(pred));
			transaction.hset(statementKey, HKEY_OBJECT, factory.encode(obj));
			URI objectType = factory.getObjectType(obj);
			if (objectType != null) {
				transaction.hset(statementKey, HKEY_OBJECT_DATATYPE, factory.encode(objectType));
			}
			if (obj instanceof Literal) {
				Literal literal = (Literal) obj;
				String objectLanguage = literal.getLanguage();
				if (objectLanguage != null) {
					transaction.hset(statementKey, HKEY_OBJECT_LANGUAGE, objectLanguage);
				}
			}
			
			// write indices
			transaction.sadd(subjectIndex, statementKey);
			transaction.sadd(predicateIndex, statementKey);
			transaction.sadd(objectIndex, statementKey);
			transaction.sadd(statementIndex, statementKey);
			
			// TODO put statement id in local transaction cache
		}

		// write context indices
		for (Resource context : contexts) {
			// write context index
			String contextIndex = mappingStrategy.createContextIndex(context);
			transaction.sadd(contextIndex, statementKey);
			
			// add context to context index unless it's the default context
			if (!getConnection().isDefaultContext(context)) {
				transaction.sadd(mappingStrategy.key(KEY_CONTEXTS), factory.encode(context));
			}
		}
	}

	@Override
	public void removeStatements(Resource subj, URI pred, Value obj,
			Resource[] contexts) throws SailException {
		contexts = getConnection().checkContexts(contexts);
		
		DefaultRedisTripleSource tripleSource = new DefaultRedisTripleSource(mappingStrategy, getConnection(), false);
		
		Map<Resource, Set<String>> statementsByContext = tripleSource.findStatements(subj, pred, obj, contexts);
		if ((statementsByContext == null) || statementsByContext.isEmpty()) {
			return;
		}
		String statementIndex = mappingStrategy.createKey(KEY_STATEMENTS);
		String subjectIndex = mappingStrategy.createSubjectIndex(subj);
		String predicateIndex = mappingStrategy.createPredicateIndex(pred);
		String objectIndex = mappingStrategy.createObjectIndex(obj);
		
		Transaction transaction = getActiveTransaction();
		
		for (Entry<Resource, Set<String>> entry : statementsByContext.entrySet()) {
			Resource context = entry.getKey();
			Set<String> statementKeys = entry.getValue();
			String contextIndex = mappingStrategy.createContextIndex(context);
			for (String statementKey : statementKeys) {
				transaction.srem(subjectIndex, statementKey);
				transaction.srem(predicateIndex, statementKey);
				transaction.srem(objectIndex, statementKey);
				transaction.srem(statementIndex, statementKey);
				transaction.srem(contextIndex, statementKey);
				transaction.del(statementKey);
			}
		}
	}
}
