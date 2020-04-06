package org.springframework.batch.item.redisearch;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.search.AddOptions;
import com.redislabs.lettusearch.search.Document;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import lombok.Builder;
import lombok.Setter;

public class DocumentItemWriter<K, V> extends AbstractItemStreamItemWriter<Document<K, V>> implements InitializingBean {

    @Setter
    private StatefulRediSearchConnection<K, V> connection;
    @Setter
    private boolean delete;
    @Setter
    private boolean deleteDocument;
    @Setter
    private K index;
    @Setter
    private long timeout;
    private final AddOptions options;

    @Builder
    protected DocumentItemWriter(StatefulRediSearchConnection<K, V> connection, boolean delete, boolean deleteDocument,
                                 K index, Long timeout, AddOptions options) {
        Assert.notNull(connection, "connection is required.");
        this.connection = connection;
        this.delete = delete;
        this.deleteDocument = deleteDocument;
        this.index = index;
        this.timeout = timeout == null ? RedisURI.DEFAULT_TIMEOUT : timeout;
        this.options = options;
    }

    @Override
    public void afterPropertiesSet() {
        Assert.notNull(connection, "A RediSearch connection is required.");
        Assert.notNull(index, "An index name is required.");
    }

    @Override
    public void write(List<? extends Document<K, V>> items) throws Exception {
        if (delete) {
            RediSearchAsyncCommands<K, V> commands = connection.async();
            commands.setAutoFlushCommands(false);
            List<RedisFuture<Boolean>> futures = new ArrayList<>();
            for (Document<K, V> item : items) {
                futures.add(commands.del(index, item.getId(), deleteDocument));
            }
            commands.flushCommands();
            for (RedisFuture<Boolean> future : futures) {
                future.get(timeout, TimeUnit.SECONDS);
            }
        } else {
            RediSearchAsyncCommands<K, V> commands = connection.async();
            commands.setAutoFlushCommands(false);
            List<RedisFuture<String>> futures = new ArrayList<>();
            for (Document<K, V> item : items) {
                futures.add(commands.add(index, item, options));
            }
            commands.flushCommands();
            for (RedisFuture<String> future : futures) {
                future.get(timeout, TimeUnit.SECONDS);
            }
        }
    }

}
