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

public abstract class RedisTripleSource implements TripleSource {

	protected final RedisStoreConnection connection;
	protected final boolean includeInferred;

	public RedisTripleSource(RedisStoreConnection connection, boolean includeInferred) {
		this.connection = connection;
		this.includeInferred = includeInferred;
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
	
	@Override
	public ValueFactory getValueFactory() {
		return connection.getValueFactory();
	}
	
	public abstract long size(Resource[] contexts);
	
	protected CloseableIteration<? extends Statement, QueryEvaluationException> noResult() {
		return new EmptyIteration<Statement, QueryEvaluationException>();
	}
	
	protected abstract CloseableIteration<? extends Statement, QueryEvaluationException> getStatementsBySubject(URI subjURI,
			URI pred, Value obj, Resource[] contexts);

	protected abstract CloseableIteration<? extends Statement, QueryEvaluationException> getStatementsByType(URI typeUri,
			Resource[] contexts);

	protected abstract CloseableIteration<? extends Statement, QueryEvaluationException> getStatementsByPredicate(URI pred,
			Resource[] contexts);

	protected abstract CloseableIteration<? extends Statement, QueryEvaluationException> getStatementsByValue(Value obj,
			Resource[] contexts);

	protected abstract CloseableIteration<? extends Statement, QueryEvaluationException> getAllStatements(Value obj,
			Resource[] contexts);

}
