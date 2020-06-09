package org.springframework.batch.item.redisearch;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import io.lettuce.core.RedisURI;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;
import org.springframework.batch.step.redisearch.IndexCreateStep;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.UrlResource;

import java.net.MalformedURLException;
import java.time.Duration;
import java.util.Map;

@SpringBootApplication
@EnableBatchProcessing
@EnableConfigurationProperties(RedisProperties.class)
public class SpringBatchRediSearchTestApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringBatchRediSearchTestApplication.class, args);
    }

    @Autowired
    private RedisProperties redisProperties;

    @Bean(destroyMethod = "shutdown")
    ClientResources clientResources() {
        return DefaultClientResources.create();
    }

    @Bean(destroyMethod = "shutdown")
    RediSearchClient client(ClientResources clientResources) {
        RedisURI redisURI = RedisURI.create(redisProperties.getHost(), redisProperties.getPort());
        if (redisProperties.getPassword() != null) {
            redisURI.setPassword(redisProperties.getPassword());
        }
        Duration timeout = redisProperties.getTimeout();
        if (timeout != null) {
            redisURI.setTimeout(timeout);
        }
        return RediSearchClient.create(clientResources, redisURI);
    }

    @Bean(name = "rediSearchConnection", destroyMethod = "close")
    StatefulRediSearchConnection<String, String> connection(RediSearchClient rediSearchClient) {
        return rediSearchClient.connect();
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
    IndexCreateStep indexCreateStep(JobRepository jobRepository, StatefulRediSearchConnection<String, String> connection) {
        IndexCreateStep step = IndexCreateStep.<String, String>builder().connection(connection).index(Utils.INDEX).schema(Utils.SCHEMA).build();
        step.setJobRepository(jobRepository);
        return step;
    }

}