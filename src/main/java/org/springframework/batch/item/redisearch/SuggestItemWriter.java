package org.springframework.batch.item.redisearch;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.suggest.Suggestion;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import lombok.Setter;

public class SuggestItemWriter<K, V> implements ItemWriter<Suggestion<V>>, InitializingBean {

	private @Setter StatefulRediSearchConnection<K, V> connection;
	private @Setter boolean delete;
	private final K key;
	private final boolean increment;
	private @Setter long timeout;

	protected SuggestItemWriter(StatefulRediSearchConnection<K, V> connection, boolean delete, K key, boolean increment,
			Long timeout) {
		Assert.notNull(connection, "connection is required.");
		this.connection = connection;
		this.delete = delete;
		this.key = key;
		this.increment = increment;
		this.timeout = timeout == null ? RedisURI.DEFAULT_TIMEOUT : timeout;
	}

	@Override
	public void write(List<? extends Suggestion<V>> items) throws Exception {
		if (delete) {
			RediSearchAsyncCommands<K, V> commands = connection.async();
			commands.setAutoFlushCommands(false);
			List<RedisFuture<Boolean>> futures = new ArrayList<>();
			for (Suggestion<V> item : items) {
				futures.add(commands.sugdel(key, item.getString()));
			}
			commands.flushCommands();
			for (RedisFuture<Boolean> future : futures) {
				future.get(timeout, TimeUnit.SECONDS);
			}
		} else {
			RediSearchAsyncCommands<K, V> commands = connection.async();
			commands.setAutoFlushCommands(false);
			List<RedisFuture<Long>> futures = new ArrayList<>();
			for (Suggestion<V> item : items) {
				futures.add(commands.sugadd(key, item, increment));
			}
			commands.flushCommands();
			for (RedisFuture<Long> future : futures) {
				future.get(timeout, TimeUnit.SECONDS);
			}
		}
	}

	@Override
	public void afterPropertiesSet() {
		Assert.notNull(connection, "A RediSearch connection is required.");
		Assert.notNull(key, "A key is required.");
	}

}
