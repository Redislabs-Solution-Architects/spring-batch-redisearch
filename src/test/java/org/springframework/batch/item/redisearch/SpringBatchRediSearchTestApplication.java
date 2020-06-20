package org.springframework.batch.item.redisearch;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import io.lettuce.core.RedisURI;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;
import org.springframework.batch.step.redisearch.IndexCreateStep;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.UrlResource;

import java.net.MalformedURLException;
import java.util.Map;

@SpringBootApplication
@EnableBatchProcessing
@EnableConfigurationProperties(RedisProperties.class)
public class SpringBatchRediSearchTestApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringBatchRediSearchTestApplication.class, args);
    }

    @Bean
    RedisURI redisURI() {
        return RedisURI.builder().withHost("localhost").build();
    }

    @Bean
    StatefulRediSearchConnection<String, String> connection(RedisURI redisURI) {
        return RediSearchClient.create(redisURI).connect();
    }

    @Bean
    FlatFileItemReader<Map<String, String>> fileReader() throws MalformedURLException {
        FlatFileItemReaderBuilder<Map<String, String>> fileReader = new FlatFileItemReaderBuilder<>();
        fileReader.name("csv-reader");
        fileReader.resource(new UrlResource("https://git.io/fjxPs"));
        fileReader.strict(true);
        fileReader.saveState(false);
        fileReader.linesToSkip(1);
        fileReader.fieldSetMapper(new MapFieldSetMapper());
        fileReader.recordSeparatorPolicy(new DefaultRecordSeparatorPolicy());
        FlatFileItemReaderBuilder.DelimitedBuilder<Map<String, String>> delimitedBuilder = fileReader.delimited();
        delimitedBuilder.names(",abv,ibu,id,name,style,brewery_id,ounces".split(","));
        return fileReader.build();
    }

    @Bean
    IndexCreateStep<String, String> indexCreateStep(JobRepository jobRepository) {
        IndexCreateStep<String, String> step = IndexCreateStep.builder().redisURI(redisURI()).index(Utils.INDEX).schema(Utils.SCHEMA).build();
        step.setJobRepository(jobRepository);
        return step;
    }

}