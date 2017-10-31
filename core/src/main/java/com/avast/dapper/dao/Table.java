package com.avast.dapper.dao;

import com.datastax.driver.core.ConsistencyLevel;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Table {
    /**
     * Keyspace of the table.
     */
    String keyspace() default "";

    /**
     * Name of the table in DB,
     */
    String name();

    /**
     * The default consistency level to use for the write operations. Can be overridden for specific call.
     */
    ConsistencyLevel defaultWriteConsistency() default ConsistencyLevel.ONE;

    /**
     * The default consistency level to use for the read operations. Can be overridden for specific call.
     */
    ConsistencyLevel defaultReadConsistency() default ConsistencyLevel.ONE;

    /**
     * The default TTL for write operations. Can be overridden for specific call.
     */
    int defaultWriteTTL() default 0;
}
