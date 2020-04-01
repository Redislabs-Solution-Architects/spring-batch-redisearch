package org.springframework.batch.item.redisearch;

import java.util.Iterator;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.search.Document;
import com.redislabs.lettusearch.search.SearchOptions;

import lombok.Builder;
import lombok.Setter;

public class DocumentItemReader<K, V> extends AbstractItemCountingItemStreamItemReader<Document<K, V>> {

	private @Setter StatefulRediSearchConnection<K, V> connection;
	private @Setter String index;
	private @Setter String query;
	private @Setter SearchOptions options;
	private Iterator<Document<K, V>> results;

	@Builder
	protected DocumentItemReader(int currentItemCount, Integer maxItemCount, Boolean saveState,
			StatefulRediSearchConnection<K, V> connection, String index, String query, SearchOptions options) {
		setName(ClassUtils.getShortName(getClass()));
		setCurrentItemCount(currentItemCount);
		setMaxItemCount(maxItemCount == null ? Integer.MAX_VALUE : maxItemCount);
		setSaveState(saveState == null ? true : saveState);
		Assert.state(connection != null, "An instance of StatefulRediSearchConnection is required.");
		this.connection = connection;
		this.index = index;
		this.query = query;
		this.options = options;
	}

	@Override
	protected void doOpen() {
		this.results = connection.sync().search(index, query, options).iterator();
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
