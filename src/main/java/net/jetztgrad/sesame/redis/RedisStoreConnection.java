package net.jetztgrad.sesame.redis;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.ConvertingIteration;
import info.aduna.iteration.ExceptionConvertingIteration;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.openrdf.model.Namespace;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.NamespaceImpl;
import org.openrdf.model.impl.URIImpl;
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
import static net.jetztgrad.sesame.redis.RedisKey.*;

public class RedisStoreConnection extends NotifyingSailConnectionBase implements
		NotifyingSailConnection {

	protected final RedisStore redisStore;
	protected final Jedis jedis;
	protected Transaction transaction = null;

	public RedisStoreConnection(RedisStore store) {
		super(store);
		this.redisStore = store;
		this.jedis = store.getJedisClient();
	}
	
	public RedisStore getRedisStore() {
		return redisStore;
	}
	
	protected Jedis getJedisClient() {
		return jedis;
	}
	
	@Override
	protected void closeInternal() throws SailException {
		redisStore.releaseJedisClient(jedis);
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
			RedisTripleSource tripleSource = new RedisTripleSource(redisStore, this, includeInferred, transactionActive());
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
		Set<String> contexts = jedis.smembers(getRedisMapping().getSystemKey(CONTEXTS));
		CollectionIteration<String, SailException> contextsIteration = new CollectionIteration<>(contexts);
		return new ConvertingIteration<String, Resource, SailException>(contextsIteration) {
			@Override
			protected Resource convert(String context)
					throws SailException {
				return new URIImpl(context);
			}
		};
	}

	@Override
	protected CloseableIteration<? extends Statement, SailException> getStatementsInternal(
			Resource subj, URI pred, Value obj, boolean includeInferred,
			Resource... contexts) throws SailException {
		try {
			RedisTripleSource tripleSource = new RedisTripleSource(redisStore, this, includeInferred, transactionActive());
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

	@Override
	protected long sizeInternal(Resource... contexts) throws SailException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	protected void startTransactionInternal() throws SailException {
		if (isActive()) {
			throw new SailException("Already have active transaction");
		}
		// start transaction
		transaction = jedis.multi();
		// TODO coordinate with regular triple writing, which is a non-atomic operation
	}

	@Override
	protected void commitInternal() throws SailException {
		verifyIsActive();
		// execute transaction and reset the transaction field
		transaction.exec();
		transaction = null;
	}

	@Override
	protected void rollbackInternal() throws SailException {
		verifyIsActive();
		// discard transaction and reset the transaction field
		transaction.discard();
		transaction = null;
	}

	@Override
	protected void addStatementInternal(Resource subj, URI pred, Value obj,
			Resource... contexts) throws SailException {
		// TODO Auto-generated method stub

	}

	@Override
	protected void removeStatementsInternal(Resource subj, URI pred, Value obj,
			Resource... contexts) throws SailException {
		// TODO Auto-generated method stub

	}

	@Override
	protected void clearInternal(Resource... contexts) throws SailException {
		// TODO find better command than flushDB, als this deletes ALL data!
		//jedis.flushDB();
		throw new SailException("clear is not currently supported!");
	}

	@Override
	protected CloseableIteration<? extends Namespace, SailException> getNamespacesInternal()
			throws SailException {
		// lookup namespaces from hash key "namespaces"
		Map<String, String> namespaces = jedis.hgetAll(getRedisMapping().getSystemKey(NAMESPACES));
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
		String namespace = jedis.hget(getRedisMapping().getSystemKey(NAMESPACES), prefix);
		return namespace;
	}

	@Override
	protected void setNamespaceInternal(String prefix, String name)
			throws SailException {
		verifyIsActive();
		// set namespace in hash key "namespaces"
		transaction.hset(getRedisMapping().getSystemKey(NAMESPACES), prefix, name);
	}

	@Override
	protected void removeNamespaceInternal(String prefix) throws SailException {
		verifyIsActive();
		// delete namespace from hash key "namespaces"
		transaction.hdel(getRedisMapping().getSystemKey(NAMESPACES), prefix);
	}

	@Override
	protected void clearNamespacesInternal() throws SailException {
		verifyIsActive();
		// delete namespaces from hash key "namespaces"
		transaction.del(getRedisMapping().getSystemKey(NAMESPACES));
	}
	
	public RedisMapping getRedisMapping() {
		return redisStore.getRedisMapping();
	}
}
