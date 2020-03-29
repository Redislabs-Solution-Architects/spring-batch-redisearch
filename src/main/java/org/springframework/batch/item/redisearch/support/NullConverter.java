package org.springframework.batch.item.redisearch.support;

import org.springframework.core.convert.converter.Converter;

public class NullConverter<S, T> implements Converter<S, T> {

	@Override
	public T convert(S source) {
		return null;
	}

}
