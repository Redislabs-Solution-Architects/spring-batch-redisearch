package org.springframework.batch.item.redisearch;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;
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
public class TestApplication {

    public static void main(String[] args) {
        SpringApplication.run(TestApplication.class, args);
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

}