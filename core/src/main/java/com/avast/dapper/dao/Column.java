package com.avast.dapper.dao;

import com.avast.dapper.CqlType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Column {
    /**
     * Custom name of this column.
     */
    String name() default "";

    /**
     * CQL type of this column.
     */
    Class<? extends CqlType> cqlType();
}
