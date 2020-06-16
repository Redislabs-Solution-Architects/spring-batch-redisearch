package org.springframework.batch.item.redisearch.support;

import com.redislabs.lettusearch.search.Document;
import lombok.Builder;
import lombok.NonNull;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.core.convert.converter.Converter;

import java.util.Map;

@Builder
public class DocumentItemProcessor<K, V> implements ItemProcessor<Map<K, V>, Document<K, V>> {

    @NonNull
    private final Converter<Map<K, V>, K> idConverter;
    @NonNull
    private final Converter<Map<K, V>, Double> scoreConverter;
    private final Converter<Map<K, V>, V> payloadConverter;

    @Override
    public Document<K, V> process(Map<K, V> item) {
        Double score = scoreConverter.convert(item);
        if (score == null) {
            return null;
        }
        Document<K, V> document = new Document<>();
        document.setId(idConverter.convert(item));
        document.setScore(score);
        if (payloadConverter != null) {
            document.setPayload(payloadConverter.convert(item));
        }
        document.putAll(item);
        return document;
    }

}
