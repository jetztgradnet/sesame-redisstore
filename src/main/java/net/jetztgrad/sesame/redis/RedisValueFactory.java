package net.jetztgrad.sesame.redis;

import java.util.UUID;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDF;

public class RedisValueFactory extends ValueFactoryImpl {
	public static final String BNODE_PREFIX = "urn:bnode:";
	
	public Value createValue(String data, String type, String language) {
		URI typeURI = (type != null ? createURI(type) : null);
		return createValue(data, typeURI, language);
	}
	
	public Value createValue(String data, URI type, String language) {
		if (type == null) {
			return createLiteral(data, language);
		}
		if (RDF.OBJECT.equals(type)) {
			return createURI(data);
		}
		
		try {
			return createLiteral(data, type);
		}
		catch (Throwable t) {
			// unknown type, ignore
		}
		return createLiteral(data, language);
	}
	
	public URI getObjectType(Value obj) {
		if (obj instanceof URI) {
			return RDF.OBJECT;
		}
		if (obj instanceof Literal) {
			Literal literal = (Literal) obj;
			return literal.getDatatype();
		}
		return null;
	}
	
	public Resource createBNodeOrURI(String uri) {
		if (isBNode(uri)) {
			return createBNode(uri);
		}
		return createURI(uri);
	}
	
	public BNode createBNode() {
		String id = UUID.randomUUID().toString();
		return createBNode(BNODE_PREFIX + id);
	}
	
	public boolean isBNode(String uri) {
		if (uri == null) return false;
		return uri.startsWith(BNODE_PREFIX);
	}
	
	/**
	 * Get string representation of a {@link Value}.
	 * 
	 * @param object value of which to get string representation
	 *   
	 * @return string representation
	 */
	public String encode(Value object) {
		if (object == null) {
			return null;
		}
		else if (object instanceof URI) {
			URI uri = (URI) object;
			// TODO do we need to encode some part?
			return uri.stringValue();
		}
		else if (object instanceof BNode) {
			// TODO do we need to encode some part?
			BNode bnode = (BNode) object;
			return bnode.stringValue();
		}
		return object.stringValue();
	}
}
