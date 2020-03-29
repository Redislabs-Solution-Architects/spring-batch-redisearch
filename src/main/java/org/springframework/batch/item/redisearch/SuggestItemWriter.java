package org.springframework.batch.item.redisearch;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.StatefulRediSearchConnection;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import lombok.Setter;

public class SuggestItemWriter<K, V, T> implements ItemWriter<T>, InitializingBean {

	private @Setter StatefulRediSearchConnection<K, V> connection;
	private @Setter boolean delete;
	private K key;
	private Converter<T, V> itemStringMapper;
	private Converter<T, Double> itemScoreMapper;
	private boolean increment;
	private Converter<T, V> itemPayloadMapper;
	private @Setter long timeout;

	protected SuggestItemWriter(StatefulRediSearchConnection<K, V> connection, boolean delete, K key,
			Converter<T, V> itemStringMapper, Converter<T, Double> itemScoreMapper, boolean increment,
			Converter<T, V> itemPayloadMapper, Long timeout) {
		Assert.notNull(connection, "connection is required.");
		this.connection = connection;
		this.delete = delete;
		this.key = key;
		this.itemStringMapper = itemStringMapper;
		this.itemScoreMapper = itemScoreMapper;
		this.itemPayloadMapper = itemPayloadMapper;
		this.increment = increment;
		this.timeout = timeout == null ? RedisURI.DEFAULT_TIMEOUT : timeout;
	}

	@Override
	public void write(List<? extends T> items) throws Exception {
		if (delete) {
			RediSearchAsyncCommands<K, V> commands = connection.async();
			commands.setAutoFlushCommands(false);
			List<RedisFuture<Boolean>> futures = new ArrayList<>();
			for (T item : items) {
				futures.add(commands.sugdel(key, itemStringMapper.convert(item)));
			}
			commands.flushCommands();
			for (RedisFuture<Boolean> future : futures) {
				future.get(timeout, TimeUnit.SECONDS);
			}
		} else {
			RediSearchAsyncCommands<K, V> commands = connection.async();
			commands.setAutoFlushCommands(false);
			List<RedisFuture<Long>> futures = new ArrayList<>();
			for (T item : items) {
				futures.add(commands.sugadd(key, itemStringMapper.convert(item), itemScoreMapper.convert(item),
						increment, itemPayloadMapper.convert(item)));
			}
			commands.flushCommands();
			for (RedisFuture<Long> future : futures) {
				future.get(timeout, TimeUnit.SECONDS);
			}
		}
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(connection, "A RediSearch connection is required.");
		Assert.notNull(key, "A key is required.");
		Assert.notNull(itemStringMapper, "A string mapper is required.");
	}

}
