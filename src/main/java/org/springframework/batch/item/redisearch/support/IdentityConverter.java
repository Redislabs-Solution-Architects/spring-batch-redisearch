package org.springframework.batch.item.redisearch.support;

import org.springframework.core.convert.converter.Converter;

public class IdentityConverter<S, T> implements Converter<S, T> {

	@SuppressWarnings("unchecked")
	public T convert(S source) {
		return (T) source;
	}

}
