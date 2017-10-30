package com.avast.dapper.dao;

import com.datastax.driver.core.ConsistencyLevel;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Table {
    String keyspace() default "";

    String name();

    /**
     * The consistency level to use for the write operations.
     */
    ConsistencyLevel defaultWriteConsistency() default ConsistencyLevel.ONE;

    /**
     * The consistency level to use for the read operations.
     */
    ConsistencyLevel defaultReadConsistency() default ConsistencyLevel.ONE;

    int defaultWriteTTL() default 0;
}
