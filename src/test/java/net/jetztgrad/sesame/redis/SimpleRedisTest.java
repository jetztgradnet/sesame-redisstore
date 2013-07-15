package net.jetztgrad.sesame.redis;

import static org.junit.Assert.*;
import info.aduna.iteration.Iterations;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.Namespace;
import org.openrdf.sail.NotifyingSailConnection;

public class SimpleRedisTest {
	protected RedisStore store;
	protected NotifyingSailConnection connection;
	
	@Before
	public void init() throws Exception {
		store = new RedisStore();
		store.initialize();
		
		connection = store.getConnection();
	}
	
	@After
	public void cleanup() throws Exception {
		try {
			connection.close();
		}
		finally {
			// TODO delete all triples
			store.shutDown();
		}
	}

	@Test
	public void namespaces() throws Exception {
		List<Namespace> namespaces = Iterations.asList(connection.getNamespaces());
		assertTrue(namespaces.isEmpty());
		
		connection.begin();
		connection.setNamespace("ex", "http://example.org/");
		connection.commit();
		
		namespaces = Iterations.asList(connection.getNamespaces());
		assertEquals(1, namespaces.size());
		Namespace namespace = namespaces.get(0);
		assertEquals("ex", namespace.getPrefix());
		assertEquals("http://example.org/", namespace.getName());
		
		connection.begin();
		connection.removeNamespace("ex");
		connection.commit();
		
		namespaces = Iterations.asList(connection.getNamespaces());
		assertTrue(namespaces.isEmpty());
	}
}
