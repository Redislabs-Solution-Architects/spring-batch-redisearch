package org.springframework.batch.item.redisearch;

import java.util.Iterator;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.suggest.SuggetOptions;
import com.redislabs.lettusearch.suggest.SuggetResult;

import lombok.Builder;
import lombok.Setter;

public class SuggestItemReader<K, V, T> extends AbstractItemCountingItemStreamItemReader<SuggetResult<V>> {

	private @Setter StatefulRediSearchConnection<K, V> connection;
	private K key;
	private V prefix;
	private SuggetOptions options;
	private Iterator<SuggetResult<V>> results;

	public SuggestItemReader() {
		setName(ClassUtils.getShortName(getClass()));
	}

	@Builder
	protected SuggestItemReader(int currentItemCount, Integer maxItemCount, Boolean saveState,
			StatefulRediSearchConnection<K, V> connection, K key, V prefix, SuggetOptions options) {
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
	protected void doOpen() throws Exception {
		this.results = connection.sync().sugget(key, prefix, options).iterator();
	}

	@Override
	protected SuggetResult<V> doRead() throws Exception {
		if (results.hasNext()) {
			return results.next();
		}
		return null;
	}

	@Override
	protected void doClose() throws Exception {
		this.results = null;
	}

}
