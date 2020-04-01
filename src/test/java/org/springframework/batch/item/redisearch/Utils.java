package org.springframework.batch.item.redisearch;

import com.redislabs.lettusearch.index.Schema;
import com.redislabs.lettusearch.index.field.NumericField;
import com.redislabs.lettusearch.index.field.PhoneticMatcher;
import com.redislabs.lettusearch.index.field.TagField;
import com.redislabs.lettusearch.index.field.TextField;

public class Utils {

	public final static String FIELD_ABV = "abv";
	public final static String FIELD_ID = "id";
	public final static String FIELD_NAME = "name";
	public final static String FIELD_STYLE = "style";
	public final static String FIELD_OUNCES = "ounces";
	public final static String INDEX = "beers";

	public final static Schema SCHEMA = Schema.builder()
			.field(TextField.builder().name(FIELD_NAME).matcher(PhoneticMatcher.English).build())
			.field(TagField.builder().name(FIELD_STYLE).sortable(true).build())
			.field(NumericField.builder().name(FIELD_ABV).sortable(true).build()).build();

}
