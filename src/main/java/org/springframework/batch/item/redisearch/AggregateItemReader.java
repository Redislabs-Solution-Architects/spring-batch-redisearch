package org.springframework.batch.item.redisearch;

import java.util.Iterator;
import java.util.Map;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.aggregate.AggregateOptions;

import lombok.Builder;
import lombok.Setter;

public class AggregateItemReader<K, V> extends AbstractItemCountingItemStreamItemReader<Map<K, V>> {

	protected @Setter StatefulRediSearchConnection<K, V> connection;
	private @Setter String index;
	private @Setter String query;
	private @Setter AggregateOptions options;
	private Iterator<Map<K, V>> results;

	@Builder
	protected AggregateItemReader(int currentItemCount, Integer maxItemCount, Boolean saveState,
			StatefulRediSearchConnection<K, V> connection, String index, String query, AggregateOptions options) {
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
		this.results = connection.sync().aggregate(index, query, options).iterator();
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

}
