# Dapper

[![Build Status](https://travis-ci.org/avast/dapper.svg?branch=master)](https://travis-ci.org/avast/dapper)
[ ![Download](https://api.bintray.com/packages/avast/maven/dapper/images/download.svg) ](https://bintray.com/avast/maven/dapper/_latestVersion)

Library for creating DAO instances for case-classes on top of the standard Java Datastax driver.

# Motivation

The [Datastax library](https://github.com/datastax/java-driver) itself provides an [object mapper](https://github.com/datastax/java-driver/tree/3.x/manual/object_mapper)
but as the whole driver is meant for Java users, the mapper works with POJO classes too.  
The goal of this library its to provide object mapping between Cassandra table and user-defined case-classes which are
more suitable for Scala programmer than the POJOs.
