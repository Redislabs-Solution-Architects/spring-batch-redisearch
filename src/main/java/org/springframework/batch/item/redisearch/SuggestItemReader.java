package org.springframework.batch.item.redisearch;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.suggest.Suggestion;
import com.redislabs.lettusearch.suggest.SuggetOptions;
import lombok.Builder;
import lombok.Setter;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.util.Iterator;

public class SuggestItemReader<K, V> extends AbstractItemCountingItemStreamItemReader<Suggestion<V>> {

	private @Setter StatefulRediSearchConnection<K, V> connection;
	private final K key;
	private final V prefix;
	private final SuggetOptions options;
	private Iterator<Suggestion<V>> results;

	@Builder
	protected SuggestItemReader(int currentItemCount, Integer maxItemCount, Boolean saveState,
			StatefulRediSearchConnection<K, V> connection, K key, V prefix, SuggetOptions options) {
		setName(ClassUtils.getShortName(getClass()));
		setCurrentItemCount(currentItemCount);
		setMaxItemCount(maxItemCount == null ? Integer.MAX_VALUE : maxItemCount);
		setSaveState(saveState == null ? true : saveState);
		Assert.state(connection != null, "An instance of StatefulRediSearchConnection is required.");
		this.connection = connection;
		this.key = key;
		this.prefix = prefix;
		this.options = options;
	}

	@Override
	protected void doOpen() {
		this.results = connection.sync().sugget(key, prefix, options).iterator();
	}

	@Override
	protected Suggestion<V> doRead() {
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
