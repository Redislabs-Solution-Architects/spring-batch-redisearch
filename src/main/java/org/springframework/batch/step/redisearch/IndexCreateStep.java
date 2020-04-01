package org.springframework.batch.step.redisearch;

import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.AbstractStep;
import org.springframework.util.Assert;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.index.CreateOptions;
import com.redislabs.lettusearch.index.Schema;

import lombok.Builder;
import lombok.Setter;

public class IndexCreateStep extends AbstractStep {

	private @Setter StatefulRediSearchConnection<?, ?> connection;
	private @Setter String index;
	private @Setter Schema schema;
	private @Setter CreateOptions options;

	@Builder
	protected IndexCreateStep(JobRepository jobRepository, boolean allowStartIfComplete, int startLimit, StepExecutionListener[] listeners, String name, StatefulRediSearchConnection<?, ?> connection, String index, Schema schema,
							  CreateOptions options) {
		super(name);
		setJobRepository(jobRepository);
		setAllowStartIfComplete(allowStartIfComplete);
		setStartLimit(startLimit);
		if (listeners!=null) {
			setStepExecutionListeners(listeners);
		}
		setConnection(connection);
		setIndex(index);
		setSchema(schema);
		setOptions(options);
	}

	@Override
	protected void doExecute(StepExecution stepExecution) {
		connection.sync().create(index, schema, options);
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.state(connection != null, "A connection is required");
		Assert.state(index != null, "An index is required");
		Assert.state(schema != null, "A schema is required");
		super.afterPropertiesSet();
	}

}
