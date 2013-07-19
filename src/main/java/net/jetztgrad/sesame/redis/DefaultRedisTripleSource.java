package net.jetztgrad.sesame.redis;

import info.aduna.iteration.CloseableIteration;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.QueryEvaluationException;

public class DefaultRedisTripleSource extends RedisTripleSource {

	public DefaultRedisTripleSource(RedisStoreConnection connection, boolean includeInferred) {
		super(connection, includeInferred);
	}
	
	@Override
	protected CloseableIteration<? extends Statement, QueryEvaluationException> getStatementsBySubject(URI subjURI,
			URI pred, Value obj, Resource[] contexts) {
		// TODO Auto-generated method stub
		return noResult();
	}

	@Override
	protected CloseableIteration<? extends Statement, QueryEvaluationException> getStatementsByType(URI typeUri,
			Resource[] contexts) {
		// TODO Auto-generated method stub
		return noResult();
	}

	@Override
	protected CloseableIteration<? extends Statement, QueryEvaluationException> getStatementsByPredicate(URI pred,
			Resource[] contexts) {
		// TODO Auto-generated method stub
		return noResult();
	}

	@Override
	protected CloseableIteration<? extends Statement, QueryEvaluationException> getStatementsByValue(Value obj,
			Resource[] contexts) {
		// TODO Auto-generated method stub
		return noResult();
	}

	@Override
	protected CloseableIteration<? extends Statement, QueryEvaluationException> getAllStatements(Value obj,
			Resource[] contexts) {
		// TODO Auto-generated method stub
		return noResult();
	}

	@Override
	public long size(Resource[] contexts) {
		// TODO Auto-generated method stub
		return 0;
	}

}
