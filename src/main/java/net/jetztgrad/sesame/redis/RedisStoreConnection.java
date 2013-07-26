package net.jetztgrad.sesame.redis;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.ConvertingIteration;
import info.aduna.iteration.ExceptionConvertingIteration;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.openrdf.model.Namespace;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.NamespaceImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.QueryRoot;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.EvaluationStrategy;
import org.openrdf.query.algebra.evaluation.impl.BindingAssigner;
import org.openrdf.query.algebra.evaluation.impl.CompareOptimizer;
import org.openrdf.query.algebra.evaluation.impl.ConjunctiveConstraintSplitter;
import org.openrdf.query.algebra.evaluation.impl.ConstantOptimizer;
import org.openrdf.query.algebra.evaluation.impl.DisjunctiveConstraintOptimizer;
import org.openrdf.query.algebra.evaluation.impl.EvaluationStrategyImpl;
import org.openrdf.query.algebra.evaluation.impl.FilterOptimizer;
import org.openrdf.query.algebra.evaluation.impl.IterativeEvaluationOptimizer;
import org.openrdf.query.algebra.evaluation.impl.OrderLimitOptimizer;
import org.openrdf.query.algebra.evaluation.impl.QueryModelNormalizer;
import org.openrdf.query.algebra.evaluation.impl.SameTermFilterOptimizer;
import org.openrdf.query.algebra.evaluation.iterator.CollectionIteration;
import org.openrdf.query.impl.EmptyBindingSet;
import org.openrdf.sail.NotifyingSailConnection;
import org.openrdf.sail.SailException;
import org.openrdf.sail.helpers.NotifyingSailConnectionBase;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

public class RedisStoreConnection extends NotifyingSailConnectionBase implements
		NotifyingSailConnection, RedisMappingKeys {

	protected final RedisStore redisStore;
	protected final RedisMappingStrategy mapping;
	private Jedis jedisReadClient = null;
	private Jedis jedisWriteClient = null;
	private Transaction transaction = null;

	public RedisStoreConnection(RedisStore store) {
		super(store);
		this.redisStore = store;
		this.mapping = store.getMappingStrategy();
	}
	
	public RedisStore getRedisStore() {
		return redisStore;
	}
	
	public RedisValueFactory getValueFactory() {
		return redisStore.getValueFactory();
	}
	
	protected Transaction getActiveTransaction() throws SailException {
		verifyIsActive();
		return transaction;
	}
	
	protected Jedis getJedisReadClient() {
		if (jedisReadClient == null) {
			jedisReadClient = redisStore.getJedisClient();
		}
		return jedisReadClient;
	}
	
	@Override
	protected void closeInternal() throws SailException {
		// return all handed out Jedis clients and discard open transactions
		try {
			// close read client
			if (jedisReadClient == null) {
				redisStore.releaseJedisClient(jedisReadClient);
			}
		}
		finally {
			// close write client and discard open transaction
			if (jedisWriteClient == null) {
				try {
					if (transaction != null) {
						transaction.discard();
					}
				}
				finally {
					redisStore.releaseJedisClient(jedisWriteClient);
				}
			}
		}
	}

	@Override
	protected CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluateInternal(TupleExpr tupleExpr,
			Dataset dataset, BindingSet bindings, boolean includeInferred) throws SailException {
		logger.trace("Incoming query model:\n{}", tupleExpr);

		// Clone the tuple expression to allow for more aggressive optimizations
		tupleExpr = tupleExpr.clone();

		if (!(tupleExpr instanceof QueryRoot)) {
			// Add a dummy root node to the tuple expressions to allow the
			// optimizers to modify the actual root node
			tupleExpr = new QueryRoot(tupleExpr);
		}

		try {
			RedisTripleSource tripleSource = createTripleSource(includeInferred);
			EvaluationStrategy strategy = getEvaluationStrategy(dataset, tripleSource);

			new BindingAssigner().optimize(tupleExpr, dataset, bindings);
			new ConstantOptimizer(strategy).optimize(tupleExpr, dataset, bindings);
			new CompareOptimizer().optimize(tupleExpr, dataset, bindings);
			new ConjunctiveConstraintSplitter().optimize(tupleExpr, dataset, bindings);
			new DisjunctiveConstraintOptimizer().optimize(tupleExpr, dataset, bindings);
			new SameTermFilterOptimizer().optimize(tupleExpr, dataset, bindings);
			new QueryModelNormalizer().optimize(tupleExpr, dataset, bindings);
			// new SubSelectJoinOptimizer().optimize(tupleExpr, dataset, bindings);
			// new QueryJoinOptimizer(new NativeEvaluationStatistics(nativeStore)).optimize(tupleExpr, dataset, bindings);
			new IterativeEvaluationOptimizer().optimize(tupleExpr, dataset, bindings);
			new FilterOptimizer().optimize(tupleExpr, dataset, bindings);
			new OrderLimitOptimizer().optimize(tupleExpr, dataset, bindings);

			logger.trace("Optimized query model:\n{}", tupleExpr);

			return strategy.evaluate(tupleExpr, EmptyBindingSet.getInstance());
		} catch (QueryEvaluationException e) {
			throw new SailException(e);
		}
	}

	protected EvaluationStrategy getEvaluationStrategy(Dataset dataset,
			RedisTripleSource tripleSource) {
		return new EvaluationStrategyImpl(tripleSource, dataset);
	}

	@Override
	protected CloseableIteration<? extends Resource, SailException> getContextIDsInternal()
			throws SailException {
		Set<String> contexts = getJedisReadClient().smembers(getRedisMapping().key(KEY_CONTEXTS));
		
		final ValueFactory factory = getValueFactory();
		CollectionIteration<String, SailException> contextsIteration = new CollectionIteration<>(contexts);
		return new ConvertingIteration<String, Resource, SailException>(contextsIteration) {
			@Override
			protected Resource convert(String context)
					throws SailException {
				return factory.createURI(context);
			}
		};
	}

	@Override
	protected CloseableIteration<? extends Statement, SailException> getStatementsInternal(
			Resource subj, URI pred, Value obj, boolean includeInferred,
			Resource... contexts) throws SailException {
		try {
			RedisTripleSource tripleSource = createTripleSource(includeInferred);
			CloseableIteration<? extends Statement, QueryEvaluationException> iter = tripleSource.getStatements(subj, pred, obj, contexts);

			return new ExceptionConvertingIteration<Statement, SailException>(iter) {
				@Override
				protected SailException convert(Exception e) {
					if (e instanceof IOException) {
						return new SailException(e);
					}
					else if (e instanceof RuntimeException) {
						throw (RuntimeException)e;
					}
					else if (e == null) {
						throw new IllegalArgumentException("e must not be null");
					}
					else {
						throw new IllegalArgumentException("Unexpected exception type: " + e.getClass());
					}
				}
			};
		}
		catch (QueryEvaluationException e) {
			throw new SailException("Unable to get statements", e);
		}
	}

	protected RedisTripleSource createTripleSource(boolean includeInferred) {
		return mapping.createTripleSource(this, includeInferred);
	}
	
	protected RedisTripleWriter createTripleWriter() {
		return mapping.createTripleWriter(this);
	}

	@Override
	protected long sizeInternal(Resource... contexts) throws SailException {
		RedisTripleSource tripleSource = createTripleSource(false);
		return tripleSource.size(contexts);
	}

	@Override
	protected void startTransactionInternal() throws SailException {
		if (isActive()) {
			throw new SailException("Already have active transaction");
		}
		
		if (jedisWriteClient == null) {
			jedisWriteClient = redisStore.getJedisClient();
		}
		try {
			transaction = jedisWriteClient.multi();
		}
		catch (Throwable t) {
			throw new SailException("failed to start transaction: " + t.getMessage(), t);
		}
	}

	@Override
	protected void commitInternal() throws SailException {
		verifyIsActive();
		
		try {
			// execute transaction and reset the transaction field
			// TODO use results somehow?
			transaction.exec();
		}
		catch (Throwable t) {
			throw new SailException("failed to commit transaction: " + t.getMessage(), t);
		}
		finally {
			transaction = null;
		}
	}

	@Override
	protected void rollbackInternal() throws SailException {
		verifyIsActive();
		
		try {
			// discard transaction and reset the transaction field
			transaction.discard();
		}
		catch (Throwable t) {
			throw new SailException("failed to rollback transaction: " + t.getMessage(), t);
		}
		finally {
			transaction = null;
		}
	}

	@Override
	protected void addStatementInternal(Resource subj, URI pred, Value obj,
			Resource... contexts) throws SailException {
		// make sure a transaction has been started
		verifyIsActive();
		
		RedisTripleWriter writer = createTripleWriter();
		
		try {
			writer.addStatement(subj, pred, obj, contexts);
		}
		catch (Throwable t) {
			throw new SailException("failed to add statement: " + t.getMessage(), t);
		}
	}

	@Override
	protected void removeStatementsInternal(Resource subj, URI pred, Value obj,
			Resource... contexts) throws SailException {
		// make sure a transaction has been started
		verifyIsActive();
		
		RedisTripleWriter writer = createTripleWriter();
		
		try {
			writer.removeStatements(subj, pred, obj, contexts);
		}
		catch (Throwable t) {
			throw new SailException("failed to remove statements: " + t.getMessage(), t);
		}
	}

	@Override
	protected void clearInternal(Resource... contexts) throws SailException {
		removeStatementsInternal(null, null, null, contexts);
	}

	@Override
	protected CloseableIteration<? extends Namespace, SailException> getNamespacesInternal()
			throws SailException {
		// lookup namespaces from hash key "namespaces"
		Map<String, String> namespaces = getJedisReadClient().hgetAll(getRedisMapping().key(KEY_NAMESPACES));
		CollectionIteration<Entry<String, String>, SailException> namespacesIteration = new CollectionIteration<>(namespaces.entrySet());
		return new ConvertingIteration<Entry<String, String>, Namespace, SailException>(namespacesIteration) {
			@Override
			protected Namespace convert(Entry<String, String> entry)
					throws SailException {
				return new NamespaceImpl(entry.getKey(), entry.getValue());
			}
		};
	}
	
	@Override
	protected String getNamespaceInternal(String prefix) throws SailException {
		// lookup namespace from hash key "namespaces"
		String namespace = getJedisReadClient().hget(getRedisMapping().key(KEY_NAMESPACES), prefix);
		return namespace;
	}

	@Override
	protected void setNamespaceInternal(String prefix, String name)
			throws SailException {
		verifyIsActive();
		// set namespace in hash key "namespaces"
		transaction.hset(getRedisMapping().key(KEY_NAMESPACES), prefix, name);
	}

	@Override
	protected void removeNamespaceInternal(String prefix) throws SailException {
		verifyIsActive();
		// delete namespace from hash key "namespaces"
		transaction.hdel(getRedisMapping().key(KEY_NAMESPACES), prefix);
	}

	@Override
	protected void clearNamespacesInternal() throws SailException {
		verifyIsActive();
		// delete namespaces from hash key "namespaces"
		transaction.del(getRedisMapping().key(KEY_NAMESPACES));
	}
	
	public RedisMappingStrategy getRedisMapping() {
		return redisStore.getMappingStrategy();
	}

	public Resource getDefaultContext() {
		return redisStore.getDefaultContext();
	}
	
	/**
	 * If the specified array of contexts is <code>null</code> or empty, all contexts are return.
	 * 
	 * @param contexts contexts to check
	 * 
	 * @return specified contexts or all contexts
	 */
	public Resource[] checkContexts(Resource... contexts) {
		/*
		 * from SailConnection#getStatements():
		 * @param contexts
		 *        The context(s) to get the data from. Note that this parameter is a
		 *        vararg and as such is optional. If no contexts are specified the
		 *        method operates on the entire repository. A <tt>null</tt> value can
		 *        be used to match context-less statements.
		 */
		Set<Resource> contextURIs = new HashSet<Resource>();
		if ((contexts == null) || (contexts.length == 0)) {
			// return all contexts
			Set<Resource> allContexts = getAllContexts();
			contextURIs.addAll(allContexts);
			contextURIs.add(getDefaultContext());
		}
		else {
			for (Resource resource : contexts) {
				if (resource == null) {
					contextURIs.add(getDefaultContext());
				}
				else {
					contextURIs.add(resource);
				}
			}
		}
		contexts = (Resource[]) contextURIs.toArray(new Resource[contextURIs.size()]);
		return contexts;
	}
	
	/**
	 * Get all contexts in this triple store
	 * 
	 * @return set of all context {@link URI}s
	 */
	public Set<Resource> getAllContexts() {
		Set<String> contexts = getJedisReadClient().smembers(getRedisMapping().key(KEY_CONTEXTS));
		final ValueFactory factory = getValueFactory();
		Set<Resource> contextURIs = new HashSet<Resource>();
		for (String context : contexts) {
			contextURIs.add(factory.createURI(context));
		}
		return contextURIs;
	}

	public boolean isDefaultContext(Resource context) {
		if (context == null) return false;
		return context.equals(getDefaultContext());
	}
}
