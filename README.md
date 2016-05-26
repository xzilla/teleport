# teleport
[![Build Status](https://travis-ci.org/pagarme/teleport.svg?branch=master)](https://travis-ci.org/pagarme/teleport)

A trigger-based Postgres replicator that performs DDL migrations by diffing
schema changes and replicating real-time data changes based on DML triggers. In
other words, a complete replicator that works without any special permissions
on the database, just like the ones you don't have in AWS RDS.

Yes, you read it right

## How it works

In a configurable time interval, teleport diffs the current schema and
replicate new tables, columns, indexes and so on from the source to the target.
Inserted, updated or deleted rows are detected by triggers on the source, which
generate events that teleport transform into batches for the appropriate
targets.

If teleport fails to apply a batch of new/updated rows due to a schema change
that is not reflected on target yet, it will queue the batch, apply the schema
change and then apply the failed batches again.  This ensures consistency on
the data even after running migrations and changing the source schema.

Currently only source databases of Postgres versions >= 9.3 are supported. This is partly due to "IF NOT EXISTS" clauses
in `data/sql/setup.sql` and also due to the use of 9.3 json aggregation functions such as `json_agg`

## Features

All the features above are replicatable by teleport:

- INSERT/UPDATE/DELETE rows
- Tables/columns
- Composite types
- Enums
- Schemas
- Functions
- Indexes
- Extensions

## Install

```
go get -u github.com/pagarme/teleport
```

## Getting started

Each running instance of teleport is responsible for managing a host, exposing
a HTTP API to receive batches from other instances. For a master-slave
replication you should run one teleport instance for the source host (master)
and other for the target host (slave), and set the API of the target as the
destination for the data fetched from the source.

### Configuring the source instance

For the source, create a config file named `source_config.yml`:

```yml
batch_size: 10000
processing_intervals:
  batcher: 100
  transmitter: 100
  applier: 100
  vacuum: 500
  ddlwatcher: 5000
database:
  name: "finops-db"
  database: "postgres"
  hostname: "postgres.mydomain.com"
  username: "teleport"
  password: "root"
  port: 5432
server:
  hostname: "0.0.0.0"
  port: 3000
targets:
  my-target:
    target_expression: "public.*"
    endpoint:
      hostname: "target.mydomain.com"
      port: 3001
    apply_schema: "test"
```

For each `target` under the `targets` section, it's possible to define a
`target_expression`, which defines what tables will be replicated. The
expression should be schema-qualified.

You should also set a `apply_schema`, which defines in what schema the data
will be applied in the target, and a `endpoint` of the target teleport
instance.

### Configuring the target instance

For the target, create a config file named `target_config.yml`:

```yml
batch_size: 10000
processing_intervals:
  batcher: 100
  transmitter: 100
  applier: 100
  vacuum: 500
  ddlwatcher: 5000
database:
  name: "my-target"
  database: "postgres"
  hostname: "postgres-replica.mydomain.com"
  username: "teleport"
  password: "root"
  port: 5432
server:
  hostname: "target.mydomain.com"
  port: 3001
```

You may have noted this config file does not include a `targets` section,
simply because this instance will not be the source for any host. You can,
however, use a instance as both source and target by simply including a
`targets` section.

### Initial load

It's possible to generate initial-load batches on the source that will be
transmitted to the target. To do a initial-load, run on source:

```
$ teleport -config source_config.yml -mode initial-load -load-target my-target
```

This will create batches on the source that will be transmitted to `my-target`
as soon as teleport starts running.

### Starting up

You may start instances before the end of the initial load.  This will
replicate data as it's extracted from the source to the target, and further
modifications will be replicated and applied later on.

On source, teleport will diff, group and batch events and transmit batches to
the target. On the target, batches will be applied on the same order as they
ocurred on the source.

On source, run:

```
$ teleport -config source_config.yml
```

On target, run:

```
$ teleport -config target_config.yml
```

Teleport is now up and running! \o/

## Performance

We've been using teleport to replicate a roughly large production database
(150GB) with ~50 DML updates per second and performance is pretty satisfying.
Under our normal load, each teleport instance uses ~150MB of memory and not
significant CPU usage nor spikes.

As teleport relies on (very light) triggers for data replication, the source
database performance may be slightly affected, but impacts were negligible for
our use cases.

Initial load uses Postgres' `COPY FROM` to load data, which makes it __very__
fast. The initial load of our entire 150GB database took under ~14 hours using
the `db.m4.xlarge` RDS instance for source and target.

## Tests

```
$ docker-compose run test
```

## License

The MIT license.
