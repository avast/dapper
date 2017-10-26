package com.avast.dapper.dao;

import com.datastax.driver.core.DataType;

public @interface DbType {
    DataType.Name name();
}
