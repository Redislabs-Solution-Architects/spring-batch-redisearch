package org.springframework.batch.item.redisearch;

import com.redislabs.lettusearch.RediSearchUtils;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.index.IndexInfo;
import com.redislabs.lettusearch.search.Document;
import com.redislabs.lettusearch.search.Limit;
import com.redislabs.lettusearch.search.SearchOptions;
import com.redislabs.lettusearch.suggest.Suggestion;
import com.redislabs.lettusearch.suggest.SuggetOptions;
import io.lettuce.core.RedisURI;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.step.redisearch.IndexCreateStep;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@SpringBootTest(classes = SpringBatchRediSearchTestApplication.class)
@RunWith(SpringRunner.class)
public class SpringBatchRediSearchIntegrationTest {

    private final static int BEER_COUNT = 2410;
    private final static int BEER_NAME_COUNT = 2304;

    @Autowired
    private JobLauncher jobLauncher;
    @Autowired
    private JobBuilderFactory jobBuilderFactory;
    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    @Autowired
    private RedisURI redisURI;
    @Autowired
    private StatefulRediSearchConnection<String, String> connection;
    @Autowired
    private FlatFileItemReader<Map<String, String>> fileReader;
    @Autowired
    private IndexCreateStep<String, String> indexCreateStep;

    @BeforeEach
    public void flush() {
        connection.sync().flushall();
    }

    private <I, O> void run(String name, ItemReader<I> reader, ItemProcessor<I, O> processor, ItemWriter<O> writer, Step... steps) throws Exception {
        TaskletStep taskletStep = stepBuilderFactory.get(name + "-step").<I, O>chunk(10).reader(reader).processor(processor).writer(writer).build();
        Job readJob = job(name, steps(steps, taskletStep));
        jobLauncher.run(readJob, new JobParameters());
    }

    private Step[] steps(Step[] steps, TaskletStep taskletStep) {
        if (steps == null || steps.length == 0) {
            return new Step[]{taskletStep};
        }
        Step[] allSteps = Arrays.copyOf(steps, steps.length + 1);
        allSteps[allSteps.length - 1] = taskletStep;
        return allSteps;
    }

    private Job job(String name, Step... steps) {
        SimpleJobBuilder builder = jobBuilderFactory.get(name + "-job").start(steps[0]);
        if (steps.length > 1) {
            for (int index = 1; index < steps.length; index++) {
                builder.next(steps[index]);
            }
        }
        return builder.build();
    }

    private void writeDocuments(String name) throws Exception {
        RediSearchItemWriter<String, String> writer = RediSearchItemWriter.builder().redisURI(redisURI).index(Utils.INDEX).build();
        run(name, fileReader, new MapDocumentProcessor(), writer, indexCreateStep);
    }

    @Test
    public void testDocumentWriter() throws Exception {
        writeDocuments("test-document-writer");
        IndexInfo info = RediSearchUtils.getInfo(connection.sync().ftInfo(Utils.INDEX));
        Assertions.assertEquals(BEER_COUNT, info.getNumDocs());
    }

    @Test
    public void testDocumentReader() throws Exception {
        writeDocuments("test-document-reader-init");
        RediSearchItemReader<String, String> reader = RediSearchItemReader.builder().redisURI(redisURI).index(Utils.INDEX).query("*").searchOptions(SearchOptions.builder().limit(Limit.builder().num(3000).build()).build()).build();
        List<Document<String, String>> docs = new ArrayList<>();
        run("test-document-reader", reader, null, (ItemWriter<Document<String, String>>) docs::addAll);
        Assertions.assertEquals(BEER_COUNT, docs.size());
    }


    private void writeSuggestions(String name) throws Exception {
        RediSearchSuggestItemWriter<String, String> writer = RediSearchSuggestItemWriter.builder().redisURI(redisURI).key(Utils.SUGGEST_KEY).build();
        run(name, fileReader, new MapSuggestionProcessor(), writer);
    }

    @Test
    public void testSuggestionWriter() throws Exception {
        writeSuggestions("test-suggestion-writer");
        Long suglen = connection.sync().suglen(Utils.SUGGEST_KEY);
        Assertions.assertEquals(BEER_NAME_COUNT, suglen);
    }

    @Test
    public void testSuggestionReader() throws Exception {
        writeSuggestions("test-suggestion-reader-init");
        RediSearchSuggestItemReader<String, String> reader = RediSearchSuggestItemReader.builder().redisURI(redisURI).key(Utils.SUGGEST_KEY).prefix("fren").suggetOptions(SuggetOptions.builder().fuzzy(true).build()).build();
        List<Suggestion<String>> suggestions = new ArrayList<>();
        run("test-suggestion-reader", reader, null, (ItemWriter<Suggestion<String>>) suggestions::addAll);
        Assertions.assertEquals(5, suggestions.size());
    }

}