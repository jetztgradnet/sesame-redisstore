package net.jetztgrad.sesame.redis;

import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryTest;
import org.openrdf.repository.sail.SailRepository;

/**
 * <p>
 * Run Redis like this for tests without persistence: 
 * <pre>
 * src/redis-server /dev/null --loglevel debug --logfile stdout
 * </pre>
 * </p>
 * @author wschell
 */
public class RedisStoreTest extends RepositoryTest {
	@Rule
	public TemporaryFolder tempDir = new TemporaryFolder();
	
	@Override
	protected Repository createRepository() throws Exception {
		// TODO provide configuration
		//Path dataDir = tempDir.newFolder("redis").toPath();
		return new SailRepository(new RedisStore());
	}

}
