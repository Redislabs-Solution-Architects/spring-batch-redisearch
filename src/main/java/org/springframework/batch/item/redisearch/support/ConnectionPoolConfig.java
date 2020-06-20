package org.springframework.batch.item.redisearch.support;

import lombok.Builder;
import lombok.Data;
import org.apache.commons.pool2.impl.BaseObjectPoolConfig;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

@Data
@Builder
public class ConnectionPoolConfig {

    @Builder.Default
    private boolean lifo = BaseObjectPoolConfig.DEFAULT_LIFO;
    @Builder.Default
    private boolean fairness = BaseObjectPoolConfig.DEFAULT_FAIRNESS;
    @Builder.Default
    private long maxWaitMillis = BaseObjectPoolConfig.DEFAULT_MAX_WAIT_MILLIS;
    @Builder.Default
    private long minEvictableIdleTimeMillis = BaseObjectPoolConfig.DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS;
    @Builder.Default
    private long evictorShutdownTimeoutMillis = BaseObjectPoolConfig.DEFAULT_EVICTOR_SHUTDOWN_TIMEOUT_MILLIS;
    @Builder.Default
    private long softMinEvictableIdleTimeMillis = BaseObjectPoolConfig.DEFAULT_SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS;
    @Builder.Default
    private int numTestsPerEvictionRun = BaseObjectPoolConfig.DEFAULT_NUM_TESTS_PER_EVICTION_RUN;
    @Builder.Default
    private String evictionPolicyClassName = BaseObjectPoolConfig.DEFAULT_EVICTION_POLICY_CLASS_NAME;
    @Builder.Default
    private boolean testOnCreate = BaseObjectPoolConfig.DEFAULT_TEST_ON_CREATE;
    @Builder.Default
    private boolean testOnBorrow = BaseObjectPoolConfig.DEFAULT_TEST_ON_BORROW;
    @Builder.Default
    private boolean testOnReturn = BaseObjectPoolConfig.DEFAULT_TEST_ON_RETURN;
    @Builder.Default
    private boolean testWhileIdle = BaseObjectPoolConfig.DEFAULT_TEST_WHILE_IDLE;
    @Builder.Default
    private long timeBetweenEvictionRunsMillis = BaseObjectPoolConfig.DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS;
    @Builder.Default
    private boolean blockWhenExhausted = BaseObjectPoolConfig.DEFAULT_BLOCK_WHEN_EXHAUSTED;
    @Builder.Default
    private boolean jmxEnabled = BaseObjectPoolConfig.DEFAULT_JMX_ENABLE;
    @Builder.Default
    private String jmxNamePrefix = BaseObjectPoolConfig.DEFAULT_JMX_NAME_PREFIX;
    @Builder.Default
    private String jmxNameBase = BaseObjectPoolConfig.DEFAULT_JMX_NAME_BASE;
    @Builder.Default
    private int maxTotal = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;
    @Builder.Default
    private int minIdle = GenericObjectPoolConfig.DEFAULT_MIN_IDLE;
    @Builder.Default
    private int maxIdle = GenericObjectPoolConfig.DEFAULT_MAX_IDLE;

    public <T> GenericObjectPoolConfig<T> config() {
        GenericObjectPoolConfig<T> config = new GenericObjectPoolConfig<>();
        config.setMaxTotal(maxTotal);
        config.setMinIdle(minIdle);
        config.setMaxIdle(maxIdle);
        config.setBlockWhenExhausted(blockWhenExhausted);
        config.setEvictorShutdownTimeoutMillis(evictorShutdownTimeoutMillis);
        config.setEvictionPolicyClassName(evictionPolicyClassName);
        config.setFairness(fairness);
        config.setJmxEnabled(jmxEnabled);
        config.setJmxNameBase(jmxNameBase);
        config.setJmxNamePrefix(jmxNamePrefix);
        config.setLifo(lifo);
        config.setMaxWaitMillis(maxWaitMillis);
        config.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);
        config.setNumTestsPerEvictionRun(numTestsPerEvictionRun);
        config.setSoftMinEvictableIdleTimeMillis(softMinEvictableIdleTimeMillis);
        config.setTestOnBorrow(testOnBorrow);
        config.setTestOnCreate(testOnCreate);
        config.setTestOnReturn(testOnReturn);
        config.setTestWhileIdle(testWhileIdle);
        config.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
        return config;
    }

}
