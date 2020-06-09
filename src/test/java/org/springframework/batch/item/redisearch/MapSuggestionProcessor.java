package org.springframework.batch.item.redisearch;

import com.redislabs.lettusearch.suggest.Suggestion;
import org.springframework.batch.item.ItemProcessor;

import java.util.Map;

public class MapSuggestionProcessor implements ItemProcessor<Map<String, String>, Suggestion<String>> {

    @Override
    public Suggestion<String> process(Map<String, String> item) {
        return Suggestion.builder().string(item.get(Utils.NAME)).build();
    }

}
