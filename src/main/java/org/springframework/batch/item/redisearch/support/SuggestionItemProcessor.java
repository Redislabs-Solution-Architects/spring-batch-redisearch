package org.springframework.batch.item.redisearch.support;

import com.redislabs.lettusearch.suggest.Suggestion;
import lombok.Builder;
import lombok.NonNull;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.core.convert.converter.Converter;

import java.util.Map;

@Builder
public class SuggestionItemProcessor<K, V> implements ItemProcessor<Map<K, V>, Suggestion<K>> {

    @NonNull
    private final Converter<Map<K, V>, K> stringConverter;
    @NonNull
    private final Converter<Map<K, V>, Double> scoreConverter;
    private final Converter<Map<K, V>, K> payloadConverter;

    @Override
    public Suggestion<K> process(Map<K, V> item) {
        Double score = scoreConverter.convert(item);
        if (score == null) {
            return null;
        }
        Suggestion<K> suggestion = new Suggestion<>();
        suggestion.setString(stringConverter.convert(item));
        suggestion.setScore(score);
        if (payloadConverter != null) {
            suggestion.setPayload(payloadConverter.convert(item));
        }
        return suggestion;
    }

}
