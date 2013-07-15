package net.jetztgrad.sesame.redis;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.EmptyIteration;

import org.openrdf.model.BNode;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.evaluation.TripleSource;

public class RedisTripleSource implements TripleSource {

	protected final RedisStore redisStore;
	protected final boolean includeInferred;
	protected final boolean transactionActive;
	protected final RedisStoreConnection redisStoreConnection;

	public RedisTripleSource(RedisStore redisStore, RedisStoreConnection redisStoreConnection, boolean includeInferred,
			boolean transactionActive) {
		this.redisStore = redisStore;
		this.redisStoreConnection = redisStoreConnection;
		this.includeInferred = includeInferred;
		this.transactionActive = transactionActive;
	}

	@Override
	public CloseableIteration<? extends Statement, QueryEvaluationException> getStatements(
			Resource subj, URI pred, Value obj, Resource... contexts)
			throws QueryEvaluationException {
		
		// retrieve triples from Redis
		if  (subj instanceof URI) {
			URI subjURI = ((URI)subj);
			return getStatementsBySubject(subjURI, pred, obj, contexts);
		}
		else if  (subj instanceof BNode) {
			//BNode bnode = (BNode) subj;
			// TODO blank nodes are not supported
			return noResult();
		}
		else if (subj != null) {
			// unknown resource type is not supported
			return noResult();
		}
		
		// from here on, subject == null
		assert(subj == null);
		
		if (RDF.TYPE.equals(pred) && obj instanceof URI) {
			// query by type
			URI typeUri = (URI) obj;
			return getStatementsByType(typeUri, contexts);
		}
		else if (pred != null) {
			// other predicates
			return getStatementsByPredicate(pred, contexts);
		}
		
		// from here on, pred == null
		assert(pred == null);
		
		if (obj != null) {
			return getStatementsByValue(obj, contexts);
		}
		
		return getAllStatements(obj, contexts);
	}

	protected CloseableIteration<? extends Statement, QueryEvaluationException> getStatementsBySubject(URI subjURI,
			URI pred, Value obj, Resource[] contexts) {
		// TODO Auto-generated method stub
		return noResult();
	}

	protected CloseableIteration<? extends Statement, QueryEvaluationException> getStatementsByType(URI typeUri,
			Resource[] contexts) {
		// TODO Auto-generated method stub
		return noResult();
	}

	protected CloseableIteration<? extends Statement, QueryEvaluationException> getStatementsByPredicate(URI pred,
			Resource[] contexts) {
		// TODO Auto-generated method stub
		return noResult();
	}

	protected CloseableIteration<? extends Statement, QueryEvaluationException> getStatementsByValue(Value obj,
			Resource[] contexts) {
		// TODO Auto-generated method stub
		return noResult();
	}

	protected CloseableIteration<? extends Statement, QueryEvaluationException> getAllStatements(Value obj,
			Resource[] contexts) {
		// TODO Auto-generated method stub
		return noResult();
	}

	@Override
	public ValueFactory getValueFactory() {
		return redisStore.getValueFactory();
	}
	
	protected CloseableIteration<? extends Statement, QueryEvaluationException> noResult() {
		return new EmptyIteration<Statement, QueryEvaluationException>();
	}
}
