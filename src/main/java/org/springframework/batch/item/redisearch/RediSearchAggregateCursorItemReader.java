package org.springframework.batch.item.redisearch;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.aggregate.AggregateOptions;
import com.redislabs.lettusearch.aggregate.AggregateWithCursorResults;
import com.redislabs.lettusearch.aggregate.Cursor;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.batch.item.redisearch.support.RediSearchConnectionBuilder;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.util.Iterator;
import java.util.Map;

public class RediSearchAggregateCursorItemReader<K, V> extends AbstractItemCountingItemStreamItemReader<Map<K, V>> {

    private final StatefulRediSearchConnection<K, V> connection;
    private final K index;
    private final V query;
    private final AggregateOptions aggregateOptions;
    private final Cursor cursor;

    private AggregateWithCursorResults<K, V> results;
    private Iterator<Map<K, V>> iterator;

    public RediSearchAggregateCursorItemReader(StatefulRediSearchConnection<K, V> connection, K index, V query, AggregateOptions aggregateOptions, Cursor cursor) {
        setName(ClassUtils.getShortName(getClass()));
        Assert.notNull(connection, "A RediSearch connection is required.");
        Assert.notNull(index, "An index name is required.");
        Assert.notNull(query, "A query is required.");
        Assert.notNull(cursor, "A cursor is required.");
        this.connection = connection;
        this.index = index;
        this.query = query;
        this.aggregateOptions = aggregateOptions;
        this.cursor = cursor;
    }

    @Override
    protected void doOpen() {
        this.results = connection.sync().aggregate(index, query, cursor, aggregateOptions);
        this.iterator = this.results.iterator();
    }

    @Override
    protected Map<K, V> doRead() {
        if (iterator.hasNext()) {
            return iterator.next();
        }
        if (results.getCursor() == 0) {
            return null;
        }
        this.results = connection.sync().cursorRead(index, results.getCursor(), cursor.getCount());
        this.iterator = this.results.iterator();
        if (iterator.hasNext()) {
            return iterator.next();
        }
        return null;
    }

    @Override
    protected void doClose() {
        connection.sync().cursorDelete(index, results.getCursor());
        this.results = null;
        this.iterator = null;
    }

    public static RediSearchAggregateCursorItemReaderBuilder builder() {
        return new RediSearchAggregateCursorItemReaderBuilder();
    }

    @Setter
    @Accessors(fluent = true)
    public static class RediSearchAggregateCursorItemReaderBuilder extends RediSearchConnectionBuilder<RediSearchAggregateCursorItemReaderBuilder> {

        private String index;
        private String query;
        private AggregateOptions aggregateOptions;
        private Cursor cursor = Cursor.builder().build();

        public RediSearchAggregateCursorItemReader<String, String> build() {
            return new RediSearchAggregateCursorItemReader<>(connection(), index, query, aggregateOptions, cursor);
        }
    }

}
