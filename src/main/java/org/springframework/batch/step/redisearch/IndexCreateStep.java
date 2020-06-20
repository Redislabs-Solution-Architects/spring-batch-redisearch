package org.springframework.batch.step.redisearch;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.index.CreateOptions;
import com.redislabs.lettusearch.index.Schema;
import io.lettuce.core.RedisCommandExecutionException;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.step.AbstractStep;
import org.springframework.batch.item.redisearch.support.RediSearchConnectionBuilder;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

@Slf4j
public class IndexCreateStep<K, V> extends AbstractStep {

    private final StatefulRediSearchConnection<K, V> connection;
    private final K index;
    private final Schema schema;
    private final CreateOptions createOptions;
    private final boolean ignoreErrors;

    public IndexCreateStep(StatefulRediSearchConnection<K, V> connection, K index, Schema schema, CreateOptions createOptions, boolean ignoreErrors) {
        setName(ClassUtils.getShortName(getClass()));
        Assert.notNull(connection, "A RediSearch connection is required.");
        Assert.notNull(index, "An index name is required.");
        Assert.notNull(schema, "A schema is required.");
        this.connection = connection;
        this.index = index;
        this.schema = schema;
        this.createOptions = createOptions;
        this.ignoreErrors = ignoreErrors;
    }

    @Override
    protected void doExecute(StepExecution stepExecution) {
        try {
            connection.sync().create(index, schema, createOptions);
        } catch (RedisCommandExecutionException e) {
            if (ignoreErrors) {
                log.debug("Could not create index {}", index, e);
            } else {
                throw e;
            }
        }
    }

    public static IndexCreateStepBuilder builder() {
        return new IndexCreateStepBuilder();
    }

    @Setter
    @Accessors(fluent = true)
    public static class IndexCreateStepBuilder extends RediSearchConnectionBuilder<IndexCreateStepBuilder> {

        private String index;
        private Schema schema;
        private CreateOptions createOptions;
        private boolean ignoreErrors;

        public IndexCreateStep<String, String> build() {
            return new IndexCreateStep<>(connection(), index, schema, createOptions, ignoreErrors);
        }
    }

}
