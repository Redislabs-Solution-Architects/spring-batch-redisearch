package org.springframework.batch.item.redisearch;

import java.util.Map;

import org.springframework.batch.item.ItemProcessor;

import com.redislabs.lettusearch.search.Document;

public class MapDocumentProcessor implements ItemProcessor<Map<String, String>, Document<String, String>> {

	@Override
	public Document<String, String> process(Map<String, String> item) {
		Document<String, String> doc = Document.<String, String>builder().id(item.get("id")).score(1d).build();
		doc.putAll(item);
		return doc;
	}

}
