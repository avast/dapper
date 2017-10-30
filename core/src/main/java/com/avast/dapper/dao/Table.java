package com.avast.dapper.dao;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.mapping.Mapper;

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
     * The consistency level to use for the write operations provded by the {@link Mapper} class.
     */
    ConsistencyLevel writeConsistency() default ConsistencyLevel.ONE;

    /**
     * The consistency level to use for the read operations provded by the {@link Mapper} class.
     */
    ConsistencyLevel readConsistency() default ConsistencyLevel.ONE;
}
