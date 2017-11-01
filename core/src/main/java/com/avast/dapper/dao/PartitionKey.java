package com.avast.dapper.dao;

import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

@Target({ElementType.FIELD, ElementType.METHOD})
public @interface PartitionKey {
    /**
     * Index of this partition key.
     */
    int order() default 0;
}
