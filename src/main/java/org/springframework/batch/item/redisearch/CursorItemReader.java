package org.springframework.batch.item.redisearch;

import java.util.Iterator;
import java.util.Map;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.aggregate.AggregateOptions;
import com.redislabs.lettusearch.aggregate.AggregateWithCursorResults;
import com.redislabs.lettusearch.aggregate.Cursor;

import lombok.Builder;
import lombok.Setter;

public class CursorItemReader<K, V> extends AbstractItemCountingItemStreamItemReader<Map<K, V>> {

	protected @Setter StatefulRediSearchConnection<K, V> connection;
	private @Setter String index;
	private @Setter String query;
	private @Setter Cursor cursor;
	private @Setter AggregateOptions options;
	private AggregateWithCursorResults<K, V> results;
	private Iterator<Map<K, V>> iterator;

	public CursorItemReader() {
		setName(ClassUtils.getShortName(getClass()));
	}

	@Builder
	protected CursorItemReader(int currentItemCount, Integer maxItemCount, Boolean saveState,
			StatefulRediSearchConnection<K, V> connection, String index, String query, Cursor cursor,
			AggregateOptions options) {
		setCurrentItemCount(currentItemCount);
		setMaxItemCount(maxItemCount == null ? Integer.MAX_VALUE : maxItemCount);
		setSaveState(saveState == null ? true : saveState);
		Assert.state(connection != null, "An instance of StatefulRediSearchConnection is required.");
		this.connection = connection;
		this.index = index;
		this.query = query;
		this.cursor = cursor;
		this.options = options;
	}

	@Override
	protected void doOpen() throws Exception {
		this.results = connection.sync().aggregate(index, query, cursor, options);
		this.iterator = this.results.iterator();
	}

	@Override
	protected Map<K, V> doRead() throws Exception {
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
	protected void doClose() throws Exception {
		connection.sync().cursorDelete(index, results.getCursor());
		this.results = null;
		this.iterator = null;
	}

}
