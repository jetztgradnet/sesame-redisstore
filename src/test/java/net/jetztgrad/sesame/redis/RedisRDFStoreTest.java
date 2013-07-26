package net.jetztgrad.sesame.redis;

import org.junit.After;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.openrdf.sail.RDFStoreTest;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailException;

import redis.clients.jedis.Jedis;

public class RedisRDFStoreTest extends RDFStoreTest {
	@Rule
	public TemporaryFolder tempDir = new TemporaryFolder();
	
	@Override
	protected Sail createSail() throws SailException {
		// TODO provide configuration
//		Path dataDir;
//		try {
//			dataDir = tempDir.newFolder("redis").toPath();
//		} catch (IOException e) {
//			throw new SailException("failed to create data directory", e);
//		}
		RedisStore store = new RedisStore();
		store.initialize();
		
		return store;
	}
	
	@Override
	@After
	public void tearDown() throws Exception {
		try {
			// delete all DB content
			// TODO this can be removed when the Redis DB is initialized 
			// with a temporary folder as persistent storage
			Jedis jedis = ((RedisStoreConnection) con).getJedisReadClient();
			if (jedis != null) {
				jedis.flushDB();
			}
		}
		finally {
			super.tearDown();
		}
	}
}
