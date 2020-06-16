package org.springframework.batch.item.redisearch.support;

import com.redislabs.lettuce.helper.RedisOptions;
import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;

public class LettuSearchHelper {

    public static StatefulRediSearchConnection<String, String> connection(RedisOptions redisOptions) {
        return rediSearchClient(redisOptions).connect();
    }

    public static RediSearchClient rediSearchClient(RedisOptions redisOptions) {
        ClientResources clientResources = redisOptions.getClientResources();
        if (clientResources == null) {
            return RediSearchClient.create(redisOptions.getRedisURI());
        }
        return RediSearchClient.create(clientResources, redisOptions.getRedisURI());
    }

    public static GenericObjectPool<StatefulRediSearchConnection<String, String>> connectionPool(RedisOptions redisOptions) {
        RediSearchClient client = rediSearchClient(redisOptions);
        return ConnectionPoolSupport.createGenericObjectPool(client::connect, redisOptions.connectionPoolConfig());
    }
}
