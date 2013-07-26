package net.jetztgrad.sesame.redis;

public class RedisMappingFactoryImpl implements RedisMappingFactory {

	@Override
	public RedisMappingStrategy getMapping(String name) {
		if (name == null) {
			return new DefaultRedisMappingStrategy();
		}
		else if (name.equals("default")) {
			return new DefaultRedisMappingStrategy();
		}
		// TODO implement other strategies
		
		
		// unknown strategy
		return new DefaultRedisMappingStrategy();
	}

}
