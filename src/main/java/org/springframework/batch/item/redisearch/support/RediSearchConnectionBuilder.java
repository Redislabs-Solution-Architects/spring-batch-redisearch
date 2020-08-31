package org.springframework.batch.item.redisearch.support;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisURI;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.time.Duration;
import java.util.function.Supplier;

@SuppressWarnings("unchecked")
public class RediSearchConnectionBuilder<B extends RediSearchConnectionBuilder<B>> {

    private RedisURI redisURI;
    private ClientResources clientResources;
    private ClientOptions clientOptions;
    private GenericObjectPoolConfig<StatefulRediSearchConnection<String, String>> poolConfig = new GenericObjectPoolConfig<>();

    public RedisURI getRedisURI() {
        return redisURI;
    }

    public Duration getTimeout() {
        return redisURI.getTimeout();
    }

    public B redisURI(RedisURI redisURI) {
        this.redisURI = redisURI;
        return (B) this;
    }

    public B clientResources(ClientResources clientResources) {
        this.clientResources = clientResources;
        return (B) this;
    }

    public B poolConfig(GenericObjectPoolConfig<StatefulRediSearchConnection<String, String>> poolConfig) {
        this.poolConfig = poolConfig;
        return (B) this;
    }

    public B clientOptions(ClientOptions clientOptions) {
        this.clientOptions = clientOptions;
        return (B) this;
    }

    public Supplier<StatefulRediSearchConnection<String, String>> connectionSupplier() {
        return client()::connect;
    }

    public StatefulRediSearchConnection<String, String> connection() {
        return client().connect();
    }

    private RediSearchClient client() {
        RediSearchClient client = createClient(redisURI, clientResources);
        if (clientOptions != null) {
            client.setOptions(clientOptions);
        }
        return client;
    }

    private RediSearchClient createClient(RedisURI redisURI, ClientResources clientResources) {
        if (clientResources == null) {
            return RediSearchClient.create(redisURI);
        }
        return RediSearchClient.create(clientResources, redisURI);
    }

    public GenericObjectPool<StatefulRediSearchConnection<String, String>> pool() {
        return ConnectionPoolSupport.createGenericObjectPool(connectionSupplier(), poolConfig);
    }

}
