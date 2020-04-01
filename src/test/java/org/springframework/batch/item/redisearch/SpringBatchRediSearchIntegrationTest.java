package org.springframework.batch.item.redisearch;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.RediSearchUtils;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.index.IndexInfo;
import com.redislabs.lettusearch.search.Document;
import com.redislabs.lettusearch.search.Limit;
import com.redislabs.lettusearch.search.SearchOptions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.step.redisearch.IndexCreateStep;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@SpringBootTest(classes = SpringBatchRediSearchTestApplication.class)
@RunWith(SpringRunner.class)
public class SpringBatchRediSearchIntegrationTest {

	@Autowired
	private JobLauncher jobLauncher;
	@Autowired
	private JobBuilderFactory jobBuilderFactory;
	@Autowired
	private StepBuilderFactory stepBuilderFactory;
	@Autowired
	private RediSearchClient client;
	@Autowired
	private StatefulRediSearchConnection<String, String> connection;
	@Autowired
	private FlatFileItemReader<Map<String, String>> fileReader;
	@Autowired
	private IndexCreateStep indexCreateStep;

	private Job documentWriteJob() {
		DocumentItemWriter<String, String> writer = DocumentItemWriter.<String, String>builder()
				.connection(client.connect()).index(Utils.INDEX).build();
		TaskletStep writeStep = stepBuilderFactory.get("documentWriteStep")
				.<Map<String, String>, Document<String, String>>chunk(10).reader(fileReader)
				.processor(new MapDocumentProcessor()).writer(writer).build();
		return jobBuilderFactory.get("documentWriteJob").start(indexCreateStep).next(writeStep).build();
	}

	@Test
	public void testDocumentReader() throws Exception {
		connection.sync().flushall();
		jobLauncher.run(documentWriteJob(), new JobParameters());
		IndexInfo info = RediSearchUtils.getInfo(connection.sync().ftInfo(Utils.INDEX));
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