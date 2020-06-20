package org.springframework.batch.item.redisearch;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.suggest.Suggestion;
import io.lettuce.core.RedisFuture;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redisearch.support.RediSearchConnectionBuilder;
import org.springframework.util.Assert;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
public class RediSearchSuggestItemWriter<K, V> implements ItemWriter<Suggestion<V>> {

    private final GenericObjectPool<StatefulRediSearchConnection<K, V>> pool;
    private final K key;
    private final long commandTimeout;
    private final boolean delete;
    private final boolean increment;

    public RediSearchSuggestItemWriter(GenericObjectPool<StatefulRediSearchConnection<K, V>> pool, K key, Duration commandTimeout, boolean delete, boolean increment) {
        Assert.notNull(pool, "A RediSearch connection pool is required.");
        Assert.notNull(key, "A key is required.");
        Assert.notNull(commandTimeout, "Command timeout is required.");
        this.pool = pool;
        this.key = key;
        this.commandTimeout = commandTimeout.getSeconds();
        this.delete = delete;
        this.increment = increment;
    }

    @Override
    public void write(List<? extends Suggestion<V>> items) throws Exception {
        StatefulRediSearchConnection<K, V> connection = pool.borrowObject();
        try {
            RediSearchAsyncCommands<K, V> commands = connection.async();
            commands.setAutoFlushCommands(false);
            if (delete) {
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
                    futures.add(commands.sugadd(key, item, increment));
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
            future.get(commandTimeout, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            log.error("Could not execute command", e);
        } catch (TimeoutException e) {
            log.error("Command timed out", e);
        }
    }

    public static RediSearchSuggestItemWriterBuilder builder() {
        return new RediSearchSuggestItemWriterBuilder();
    }

    @Setter
    @Accessors(fluent = true)
    public static class RediSearchSuggestItemWriterBuilder extends RediSearchConnectionBuilder<RediSearchSuggestItemWriterBuilder> {

        private String key;
        private boolean delete;
        private boolean increment;

        public RediSearchSuggestItemWriter<String, String> build() {
            return new RediSearchSuggestItemWriter<>(pool(), key, getTimeout(), delete, increment);
        }
    }


}
