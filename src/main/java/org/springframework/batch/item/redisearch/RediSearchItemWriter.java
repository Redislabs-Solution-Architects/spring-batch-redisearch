package org.springframework.batch.item.redisearch;

import com.redislabs.lettuce.helper.RedisOptions;
import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.search.AddOptions;
import com.redislabs.lettusearch.search.Document;
import io.lettuce.core.RedisFuture;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redisearch.support.LettuSearchHelper;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.Assert;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
public class RediSearchItemWriter<K, V> extends AbstractItemStreamItemWriter<Document<K, V>> {

    private final GenericObjectPool<StatefulRediSearchConnection<K, V>> pool;
    private final K index;
    private final long commandTimeout;
    private final boolean delete;
    private final boolean deleteDocument;
    private final AddOptions addOptions;

    public RediSearchItemWriter(GenericObjectPool<StatefulRediSearchConnection<K, V>> pool, K index, Duration commandTimeout, boolean delete, boolean deleteDocument, AddOptions addOptions) {
        Assert.notNull(pool, "A RediSearch connection pool is required.");
        Assert.notNull(index, "An index name is required.");
        Assert.notNull(commandTimeout, "Command timeout is required.");
        this.pool = pool;
        this.index = index;
        this.commandTimeout = commandTimeout.getSeconds();
        this.delete = delete;
        this.deleteDocument = deleteDocument;
        this.addOptions = addOptions;
    }

    @Override
    public void write(List<? extends Document<K, V>> items) throws Exception {
        StatefulRediSearchConnection<K, V> connection = pool.borrowObject();
        try {
            RediSearchAsyncCommands<K, V> commands = connection.async();
            commands.setAutoFlushCommands(false);
            if (delete) {
                List<RedisFuture<Boolean>> futures = new ArrayList<>();
                for (Document<K, V> item : items) {
                    futures.add(commands.del(index, item.getId(), deleteDocument));
                }
                commands.flushCommands();
                for (RedisFuture<Boolean> future : futures) {
                    get(future);
                }
            } else {
                List<RedisFuture<String>> futures = new ArrayList<>();
                for (Document<K, V> item : items) {
                    futures.add(commands.add(index, item, addOptions));
                }
                commands.flushCommands();
                for (RedisFuture<String> future : futures) {
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

    public static RediSearchItemWriterBuilder builder() {
        return new RediSearchItemWriterBuilder();
    }

    @Setter
    @Accessors(fluent = true)
    public static class RediSearchItemWriterBuilder {

        private RedisOptions redisOptions;
        private String index;
        private boolean delete;
        private boolean deleteDocument;
        private AddOptions addOptions;

        public RediSearchItemWriter<String, String> build() {
            Assert.notNull(redisOptions, "Redis options are required");
            return new RediSearchItemWriter<>(LettuSearchHelper.connectionPool(redisOptions), index, redisOptions.getTimeout(), delete, deleteDocument, addOptions);
        }
    }


}
