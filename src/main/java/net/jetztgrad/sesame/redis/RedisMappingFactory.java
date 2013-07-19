package net.jetztgrad.sesame.redis;

public interface RedisMappingFactory {
	RedisMappingStrategy getMapping(String name);
}
