package org.springframework.batch.item.redisearch;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.search.Document;
import com.redislabs.lettusearch.search.SearchOptions;
import lombok.Builder;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.util.Iterator;

public class RediSearchDocumentItemReader<K, V> extends AbstractItemCountingItemStreamItemReader<Document<K, V>> {

    private final StatefulRediSearchConnection<K, V> connection;
    private final K index;
    private final V query;
    private final SearchOptions searchOptions;

    private Iterator<Document<K, V>> results;

    @Builder
    public RediSearchDocumentItemReader(StatefulRediSearchConnection<K, V> connection, K index, V query, SearchOptions searchOptions) {
        setName(ClassUtils.getShortName(getClass()));
        Assert.notNull(connection, "A RediSearch connection is required.");
        Assert.notNull(index, "An index name is required.");
        Assert.notNull(query, "A query is required.");
        this.connection = connection;
        this.index = index;
        this.query = query;
        this.searchOptions = searchOptions;
    }

    @Override
    protected void doOpen() {
        this.results = connection.sync().search(index, query, searchOptions).iterator();
    }

    @Override
    protected Document<K, V> doRead() {
        if (results.hasNext()) {
            return results.next();
        }
        return null;
    }

    @Override
    protected void doClose() {
        this.results = null;
    }

}
