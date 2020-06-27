package org.springframework.batch.item.redisearch;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.search.Document;
import com.redislabs.lettusearch.search.SearchOptions;
import com.redislabs.lettusearch.search.SearchResults;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.batch.item.redisearch.support.RediSearchConnectionBuilder;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.util.Iterator;

public class RediSearchItemReader<K, V> extends AbstractItemCountingItemStreamItemReader<Document<K, V>> {

    private final StatefulRediSearchConnection<K, V> connection;
    private final K index;
    private final V query;
    private final SearchOptions searchOptions;
    private final Object[] args;

    private Iterator<Document<K, V>> results;

    public RediSearchItemReader(StatefulRediSearchConnection<K, V> connection, K index, V query, SearchOptions searchOptions, Object[] args) {
        setName(ClassUtils.getShortName(getClass()));
        Assert.notNull(connection, "A RediSearch connection is required.");
        Assert.notNull(index, "An index name is required.");
        Assert.notNull(query, "A query is required.");
        this.connection = connection;
        this.index = index;
        this.query = query;
        this.searchOptions = searchOptions;
        this.args = args;
    }

    @Override
    protected void doOpen() {
        this.results = search().iterator();
    }

    private SearchResults<K, V> search() {
        if (args == null) {
            return connection.sync().search(index, query, searchOptions);
        }
        return connection.sync().search(index, query, args);
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


    public static RediSearchItemReaderBuilder builder() {
        return new RediSearchItemReaderBuilder();
    }

    @Setter
    @Accessors(fluent = true)
    public static class RediSearchItemReaderBuilder extends RediSearchConnectionBuilder<RediSearchItemReaderBuilder> {

        private String index;
        private String query;
        private SearchOptions searchOptions;
        private Object[] args;

        public RediSearchItemReader<String, String> build() {
            return new RediSearchItemReader<>(connection(), index, query, searchOptions, args);
        }
    }

}
