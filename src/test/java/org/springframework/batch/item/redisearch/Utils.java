package org.springframework.batch.item.redisearch;

import com.redislabs.lettusearch.index.Schema;
import com.redislabs.lettusearch.index.field.NumericField;
import com.redislabs.lettusearch.index.field.PhoneticMatcher;
import com.redislabs.lettusearch.index.field.TagField;
import com.redislabs.lettusearch.index.field.TextField;

public class Utils {

	public final static String ABV = "abv";
	public final static String ID = "id";
	public final static String NAME = "name";
	public final static String STYLE = "style";
	public final static String OUNCES = "ounces";
	public final static String INDEX = "beers";
	public final static String SUGGEST_KEY = "beersSuggest";

	public final static Schema SCHEMA = Schema.builder()
			.field(TextField.builder().name(NAME).matcher(PhoneticMatcher.English).build())
			.field(TagField.builder().name(STYLE).sortable(true).build())
			.field(NumericField.builder().name(ABV).sortable(true).build()).build();

}
