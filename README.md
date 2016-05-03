# teleport
[![Build Status](https://travis-ci.org/pagarme/teleport.svg?branch=master)](https://travis-ci.org/pagarme/teleport)

A trigger-based Postgres replicator that performs DDL migrations by diffing
schema changes and replicates real-time data changes based on DML triggers. In
other words, it works without any special permissions on the database, just
like AWS RDS databases. Yes, you read it right.

## Install

```
go get -u github.com/pagarme/teleport
```
