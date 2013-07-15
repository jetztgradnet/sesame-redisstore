package net.jetztgrad.sesame.redis;

import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.openrdf.sail.RDFStoreTest;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailException;

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
}
