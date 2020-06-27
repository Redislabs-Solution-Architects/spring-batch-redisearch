package org.springframework.batch.item.redisearch;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.aggregate.AggregateOptions;
import com.redislabs.lettusearch.aggregate.AggregateResults;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.batch.item.redisearch.support.RediSearchConnectionBuilder;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.util.Iterator;
import java.util.Map;

public class RediSearchAggregateItemReader<K, V> extends AbstractItemCountingItemStreamItemReader<Map<K, V>> {

    private final StatefulRediSearchConnection<K, V> connection;
    private final K index;
    private final V query;
    private final AggregateOptions aggregateOptions;
    private final Object[] args;

    private Iterator<Map<K, V>> results;

    public RediSearchAggregateItemReader(StatefulRediSearchConnection<K, V> connection, K index, V query, AggregateOptions aggregateOptions, Object[] args) {
        setName(ClassUtils.getShortName(getClass()));
        Assert.notNull(connection, "A RediSearch connection is required.");
        Assert.notNull(index, "An index name is required.");
        Assert.notNull(query, "A query is required.");
        this.connection = connection;
        this.index = index;
        this.query = query;
        this.aggregateOptions = aggregateOptions;
        this.args = args;
    }

    @Override
    protected void doOpen() {
        this.results = aggregate().iterator();
    }

    private AggregateResults<K, V> aggregate() {
        if (args == null) {
            return connection.sync().aggregate(index, query, aggregateOptions);
        }
        return connection.sync().aggregate(index, query, args);
    }

    @Override
    protected Map<K, V> doRead() {
        if (results.hasNext()) {
            return results.next();
        }
        return null;
    }

    @Override
    protected void doClose() {
        this.results = null;
    }

    public static RediSearchAggregateItemReaderBuilder builder() {
        return new RediSearchAggregateItemReaderBuilder();
    }

    @Setter
    @Accessors(fluent = true)
    public static class RediSearchAggregateItemReaderBuilder extends RediSearchConnectionBuilder<RediSearchAggregateItemReaderBuilder> {

        private String index;
        private String query;
        private AggregateOptions aggregateOptions;
        private Object[] args;

        public RediSearchAggregateItemReader<String, String> build() {
            return new RediSearchAggregateItemReader<>(connection(), index, query, aggregateOptions, args);
        }
    }

}
