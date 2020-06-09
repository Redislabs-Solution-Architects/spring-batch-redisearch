package org.springframework.batch.item.redisearch;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.RediSearchUtils;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.index.IndexInfo;
import com.redislabs.lettusearch.search.Document;
import com.redislabs.lettusearch.search.Limit;
import com.redislabs.lettusearch.search.SearchOptions;
import com.redislabs.lettusearch.suggest.Suggestion;
import com.redislabs.lettusearch.suggest.SuggetOptions;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
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
    private IndexCreateStep<String, String> indexCreateStep;

    private Job documentWriteJob() {
        GenericObjectPool<StatefulRediSearchConnection<String, String>> pool = ConnectionPoolSupport.createGenericObjectPool(client::connect, new GenericObjectPoolConfig<>());
        RediSearchDocumentItemWriter<String, String> writer = RediSearchDocumentItemWriter.<String, String>builder().pool(pool).index(Utils.INDEX).build();
        TaskletStep writeStep = stepBuilderFactory.get("documentWriteStep").<Map<String, String>, Document<String, String>>chunk(10).reader(fileReader).processor(new MapDocumentProcessor()).writer(writer).build();
        return jobBuilderFactory.get("documentWriteJob").start(indexCreateStep).next(writeStep).build();
    }

    @Test
    public void testDocumentReader() throws Exception {
        connection.sync().flushall();
        jobLauncher.run(documentWriteJob(), new JobParameters());
        IndexInfo info = RediSearchUtils.getInfo(connection.sync().ftInfo(Utils.INDEX));
        Assertions.assertEquals(2410, info.getNumDocs());
        RediSearchDocumentItemReader<String, String> reader = RediSearchDocumentItemReader.<String,String>builder().connection(client.connect()).index(Utils.INDEX).query("*").searchOptions(SearchOptions.builder().limit(Limit.builder().num(3000).build()).build()).build();
        List<Document<String, String>> docs = new ArrayList<>();
        TaskletStep readStep = stepBuilderFactory.get("readStep").<Document<String, String>, Document<String, String>>chunk(10).reader(reader).writer(docs::addAll).build();
        Job readJob = jobBuilderFactory.get("readJob").start(readStep).build();
        jobLauncher.run(readJob, new JobParameters());
        Assertions.assertEquals(2410, docs.size());
    }

    private Job suggestWriteJob() {
        GenericObjectPool<StatefulRediSearchConnection<String, String>> pool = ConnectionPoolSupport.createGenericObjectPool(client::connect, new GenericObjectPoolConfig<>());
        RediSearchSuggestItemWriter<String, String> writer = RediSearchSuggestItemWriter.<String, String>builder().pool(pool).key(Utils.SUGGEST_KEY).build();
        TaskletStep writeStep = stepBuilderFactory.get("suggestWriteStep").<Map<String, String>, Suggestion<String>>chunk(10).reader(fileReader).processor(new MapSuggestionProcessor()).writer(writer).build();
        return jobBuilderFactory.get("documentWriteJob").start(writeStep).build();
    }

    @Test
    public void testSuggestReader() throws Exception {
        connection.sync().flushall();
        jobLauncher.run(suggestWriteJob(), new JobParameters());
        Long suglen = connection.sync().suglen(Utils.SUGGEST_KEY);
        Assertions.assertEquals(2304, suglen);
        RediSearchSuggestItemReader<String, String> reader = RediSearchSuggestItemReader.<String,String>builder().connection(client.connect()).key(Utils.SUGGEST_KEY).prefix("fren").suggetOptions(SuggetOptions.builder().fuzzy(true).build()).build();
        List<Suggestion<String>> suggestions = new ArrayList<>();
        TaskletStep readStep = stepBuilderFactory.get("suggestReadStep").<Suggestion<String>, Suggestion<String>>chunk(10).reader(reader).writer(suggestions::addAll).build();
        Job readJob = jobBuilderFactory.get("readJob").start(readStep).build();
        jobLauncher.run(readJob, new JobParameters());
        Assertions.assertEquals(5, suggestions.size());

    }

}