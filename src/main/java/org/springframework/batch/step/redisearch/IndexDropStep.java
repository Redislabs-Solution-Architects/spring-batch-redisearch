package org.springframework.batch.step.redisearch;

import io.lettuce.core.RedisCommandExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.AbstractStep;
import org.springframework.util.Assert;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.index.DropOptions;

import lombok.Builder;
import lombok.Setter;

@Slf4j
public class IndexDropStep extends AbstractStep {
    @Setter
    private StatefulRediSearchConnection<?, ?> connection;
    @Setter
    private String index;
    @Setter
    private DropOptions options;
    @Setter
    private boolean ignoreErrors;

    @Builder
    protected IndexDropStep(JobRepository jobRepository, boolean allowStartIfComplete, int startLimit, StepExecutionListener[] listeners, String name, StatefulRediSearchConnection<?, ?> connection, String index,
                            DropOptions options, boolean ignoreErrors) {
        super(name);
        setJobRepository(jobRepository);
        setAllowStartIfComplete(allowStartIfComplete);
        setStartLimit(startLimit);
        if (listeners != null) {
            setStepExecutionListeners(listeners);
        }
        setConnection(connection);
        setIndex(index);
        setOptions(options);
        setIgnoreErrors(ignoreErrors);
    }

    @Override
    protected void doExecute(StepExecution stepExecution) {
        try {
            connection.sync().drop(index, options);
        } catch (RedisCommandExecutionException e) {
            if (ignoreErrors) {
                log.debug("Could not drop index {}", index, e);
            } else {
                throw e;
            }
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.state(connection != null, "A connection is required");
        Assert.state(index != null, "An index is required");
        super.afterPropertiesSet();
    }

}
