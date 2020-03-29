package org.springframework.batch.item.redisearch;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder.DelimitedBuilder;
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;
import org.springframework.batch.item.redisearch.support.IdentityConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.UrlResource;
import org.springframework.test.context.junit4.SpringRunner;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.RediSearchUtils;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.index.IndexInfo;
import com.redislabs.lettusearch.search.Document;
import com.redislabs.lettusearch.search.Limit;
import com.redislabs.lettusearch.search.SearchOptions;

@SpringBootTest(classes = BatchTestApplication.class)
@RunWith(SpringRunner.class)
public class SpringBatchIntegrationTest {

	@Autowired
	private JobLauncher jobLauncher;
	@Autowired
	private JobBuilderFactory jobBuilderFactory;
	@Autowired
	private StepBuilderFactory stepBuilderFactory;
	@Autowired
	private RediSearchClient client;

	@Test
	public void testDocumentReader() throws Exception {
		StatefulRediSearchConnection<String, String> connection = client.connect();
		connection.sync().flushall();
		connection.sync().create(Utils.INDEX, Utils.SCHEMA);
		FlatFileItemReaderBuilder<Map<String, String>> fileReader = new FlatFileItemReaderBuilder<>();
		fileReader.name("csv-reader");
		fileReader.resource(new UrlResource("https://git.io/fjxPs"));
		fileReader.strict(true);
		fileReader.saveState(false);
		fileReader.linesToSkip(1);
		fileReader.fieldSetMapper(new MapFieldSetMapper());
		fileReader.recordSeparatorPolicy(new DefaultRecordSeparatorPolicy());
		DelimitedBuilder<Map<String, String>> delimitedBuilder = fileReader.delimited();
		delimitedBuilder.names(",abv,ibu,id,name,style,brewery_id,ounces".split(","));
		DocumentItemWriter<String, String, Map<String, String>> writer = DocumentItemWriter
				.<String, String, Map<String, String>>builder().connection(client.connect()).index(Utils.INDEX)
				.itemDocIdMapper(m -> m.get("id")).itemFieldsMapper(new IdentityConverter<>()).itemScoreMapper(m -> 1d)
				.build();
		TaskletStep writeStep = stepBuilderFactory.get("writeStep").<Map<String, String>, Map<String, String>>chunk(10)
				.reader(fileReader.build()).writer(writer).build();
		Job writeJob = jobBuilderFactory.get("writeJob").start(writeStep).build();
		jobLauncher.run(writeJob, new JobParameters());
		IndexInfo info = RediSearchUtils.getInfo(connection.sync().indexInfo(Utils.INDEX));
		Assertions.assertEquals(2410, info.getNumDocs());
		DocumentItemReader<String, String> reader = DocumentItemReader.<String, String>builder()
				.connection(client.connect()).index(Utils.INDEX).query("*")
				.options(SearchOptions.builder().limit(Limit.builder().num(3000).build()).build()).build();
		List<Document<String, String>> docs = new ArrayList<>();
		TaskletStep readStep = stepBuilderFactory.get("readStep")
				.<Document<String, String>, Document<String, String>>chunk(10).reader(reader)
				.writer(l -> docs.addAll(l)).build();
		Job readJob = jobBuilderFactory.get("readJob").start(readStep).build();
		jobLauncher.run(readJob, new JobParameters());
		Assertions.assertEquals(2410, docs.size());
	}

}