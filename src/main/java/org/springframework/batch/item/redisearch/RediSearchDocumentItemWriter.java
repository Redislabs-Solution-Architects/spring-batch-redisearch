package org.springframework.batch.item.redisearch;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.search.AddOptions;
import com.redislabs.lettusearch.search.Document;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
public class RediSearchDocumentItemWriter<K, V> extends AbstractItemStreamItemWriter<Document<K, V>> {

    private final GenericObjectPool<StatefulRediSearchConnection<K, V>> pool;
    private final K index;
    private final Options options;

    @Builder
    public RediSearchDocumentItemWriter(GenericObjectPool<StatefulRediSearchConnection<K, V>> pool, K index, Options options) {
        Assert.notNull(pool, "A RediSearch connection pool is required.");
        Assert.notNull(index, "An index name is required.");
        Assert.notNull(options, "Options are required.");
        this.pool = pool;
        this.index = index;
        this.options = options;
    }

    @Override
    public void write(List<? extends Document<K, V>> items) throws Exception {
        StatefulRediSearchConnection<K, V> connection = pool.borrowObject();
        try {
            RediSearchAsyncCommands<K, V> commands = connection.async();
            commands.setAutoFlushCommands(false);
            if (options.isDelete()) {
                List<RedisFuture<Boolean>> futures = new ArrayList<>();
                for (Document<K, V> item : items) {
                    futures.add(commands.del(index, item.getId(), options.isDeleteDocument()));
                }
                commands.flushCommands();
                for (RedisFuture<Boolean> future : futures) {
                    get(future);
                }
            } else {
                List<RedisFuture<String>> futures = new ArrayList<>();
                for (Document<K, V> item : items) {
                    futures.add(commands.add(index, item, options.getAddOptions()));
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
        private boolean deleteDocument;
        @Builder.Default
        private AddOptions addOptions = AddOptions.builder().build();
        @Builder.Default
        private long commandTimeout = RedisURI.DEFAULT_TIMEOUT;
    }

    public static class RediSearchDocumentItemWriterBuilder<K, V> {

        private Options options = Options.builder().build();

    }

}
