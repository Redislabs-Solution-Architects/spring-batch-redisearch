package org.springframework.batch.item.redisearch;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.suggest.Suggestion;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ItemWriter;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
public class RediSearchSuggestItemWriter<K, V> implements ItemWriter<Suggestion<V>> {

    private final GenericObjectPool<StatefulRediSearchConnection<K, V>> pool;
    private final K key;
    private final Options options;

    @Builder
    public RediSearchSuggestItemWriter(GenericObjectPool<StatefulRediSearchConnection<K, V>> pool, K key, Options options) {
        Assert.notNull(pool, "A RediSearch connection pool is required.");
        Assert.notNull(key, "A key is required.");
        Assert.notNull(options, "Options are required.");
        this.pool = pool;
        this.key = key;
        this.options = options;
    }

    @Override
    public void write(List<? extends Suggestion<V>> items) throws Exception {
        StatefulRediSearchConnection<K, V> connection = pool.borrowObject();
        try {
            RediSearchAsyncCommands<K, V> commands = connection.async();
            commands.setAutoFlushCommands(false);
            if (options.isDelete()) {
                List<RedisFuture<Boolean>> futures = new ArrayList<>();
                for (Suggestion<V> item : items) {
                    futures.add(commands.sugdel(key, item.getString()));
                }
                commands.flushCommands();
                for (RedisFuture<Boolean> future : futures) {
                    get(future);
                }
            } else {
                List<RedisFuture<Long>> futures = new ArrayList<>();
                for (Suggestion<V> item : items) {
                    futures.add(commands.sugadd(key, item, options.isIncrement()));
                }
                commands.flushCommands();
                for (RedisFuture<Long> future : futures) {
                    get(future);
                }
            }
        } finally {
            pool.returnObject(connection);
        }
    }

    private void get(RedisFuture<?> future) throws InterruptedException {
        try {
            future.get(options.getCommandTimeout(), TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            log.error("Could not execute command", e);
        } catch (TimeoutException e) {
            log.error("Command timed out", e);
        }
    }

    @Data
    @Builder
    public static class Options {
        private boolean delete;
        private boolean increment;
        @Builder.Default
        private long commandTimeout = RedisURI.DEFAULT_TIMEOUT;
    }

    public static class RediSearchSuggestItemWriterBuilder<K, V> {

        private Options options = Options.builder().build();

    }


}
