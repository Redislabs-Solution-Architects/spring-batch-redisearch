package org.springframework.batch.item.redisearch;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.redisearch.support.NullConverter;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.search.AddOptions;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import lombok.Builder;
import lombok.Setter;

public class DocumentItemWriter<K, V, T> extends AbstractItemStreamItemWriter<T> implements InitializingBean {

	private @Setter StatefulRediSearchConnection<K, V> connection;
	private @Setter boolean delete;
	private @Setter boolean deleteDocument;
	private @Setter String index;
	private @Setter long timeout;
	private Converter<T, K> itemDocIdMapper;
	private Converter<T, Double> itemScoreMapper;
	private Converter<T, Map<K, V>> itemFieldsMapper;
	private Converter<T, V> itemPayloadMapper;
	private AddOptions options;

	@Builder
	protected DocumentItemWriter(StatefulRediSearchConnection<K, V> connection, boolean delete, boolean deleteDocument,
			String index, Long timeout, Converter<T, K> itemDocIdMapper, Converter<T, Double> itemScoreMapper,
			Converter<T, Map<K, V>> itemFieldsMapper, Converter<T, V> itemPayloadMapper, AddOptions options) {
		Assert.notNull(connection, "connection is required.");
		this.connection = connection;
		this.delete = delete;
		this.deleteDocument = deleteDocument;
		this.index = index;
		this.timeout = timeout == null ? RedisURI.DEFAULT_TIMEOUT : timeout;
		this.itemDocIdMapper = itemDocIdMapper;
		this.itemScoreMapper = itemScoreMapper;
		this.itemFieldsMapper = itemFieldsMapper;
		this.itemPayloadMapper = itemPayloadMapper;
		this.options = options;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(connection, "A RediSearch connection is required.");
		Assert.notNull(index, "An index name is required.");
		Assert.notNull(itemDocIdMapper, "A docId mapper is required.");
	}

	@Override
	public void open(ExecutionContext executionContext) {
		if (itemPayloadMapper == null) {
			itemPayloadMapper = new NullConverter<>();
		}
		super.open(executionContext);
	}

	@Override
	public void write(List<? extends T> items) throws Exception {
		if (delete) {
			RediSearchAsyncCommands<K, V> commands = connection.async();
			commands.setAutoFlushCommands(false);
			List<RedisFuture<Boolean>> futures = new ArrayList<>();
			for (T item : items) {
				futures.add(commands.del(index, itemDocIdMapper.convert(item), deleteDocument));
			}
			commands.flushCommands();
			for (RedisFuture<Boolean> future : futures) {
				future.get(timeout, TimeUnit.SECONDS);
			}
		} else {
			RediSearchAsyncCommands<K, V> commands = connection.async();
			commands.setAutoFlushCommands(false);
			List<RedisFuture<String>> futures = new ArrayList<>();
			for (T item : items) {
				futures.add(commands.add(index, itemDocIdMapper.convert(item), itemScoreMapper.convert(item),
						itemFieldsMapper.convert(item), itemPayloadMapper.convert(item), options));
			}
			commands.flushCommands();
			for (RedisFuture<String> future : futures) {
				future.get(timeout, TimeUnit.SECONDS);
			}
		}
	}

}
