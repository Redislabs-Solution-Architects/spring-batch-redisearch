package org.springframework.batch.item.redisearch;

import java.time.Duration;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import com.redislabs.lettusearch.RediSearchClient;

import io.lettuce.core.RedisURI;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;

@SpringBootApplication
@EnableBatchProcessing
@EnableConfigurationProperties(RedisProperties.class)
public class BatchTestApplication {

	public static void main(String[] args) {
		SpringApplication.run(BatchTestApplication.class, args);
	}

	@Autowired
	private RedisProperties redisProperties;

	@Bean(destroyMethod = "shutdown")
	ClientResources clientResources() {
		return DefaultClientResources.create();
	}

	@Bean(destroyMethod = "shutdown")
	RediSearchClient client(ClientResources clientResources) {
		RedisURI redisURI = RedisURI.create(redisProperties.getHost(), redisProperties.getPort());
		if (redisProperties.getPassword() != null) {
			redisURI.setPassword(redisProperties.getPassword());
		}
		Duration timeout = redisProperties.getTimeout();
		if (timeout != null) {
			redisURI.setTimeout(timeout);
		}
		return RediSearchClient.create(clientResources, redisURI);
	}

}