package org.springframework.batch.item.redisearch;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.aggregate.AggregateOptions;
import com.redislabs.lettusearch.aggregate.AggregateWithCursorResults;
import com.redislabs.lettusearch.aggregate.Cursor;
import lombok.Builder;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.util.Iterator;
import java.util.Map;

public class RediSearchCursorItemReader<K, V> extends AbstractItemCountingItemStreamItemReader<Map<K, V>> {

    private final StatefulRediSearchConnection<K, V> connection;
    private final K index;
    private final V query;
    private final Cursor cursor;
    private final AggregateOptions aggregateOptions;

    private AggregateWithCursorResults<K, V> results;
    private Iterator<Map<K, V>> iterator;

    @Builder
    public RediSearchCursorItemReader(StatefulRediSearchConnection<K, V> connection, K index, V query, Cursor cursor, AggregateOptions aggregateOptions) {
        setName(ClassUtils.getShortName(getClass()));
        Assert.notNull(connection, "A RediSearch connection is required.");
        Assert.notNull(index, "An index name is required.");
        Assert.notNull(query, "A query is required.");
        this.connection = connection;
        this.index = index;
        this.query = query;
        this.cursor = cursor;
        this.aggregateOptions = aggregateOptions;
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
        this.results = cursor == null || cursor.getCount() == null ? connection.sync().cursorRead(index, results.getCursor()) : connection.sync().cursorRead(index, results.getCursor(), (long) cursor.getCount());
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

}
