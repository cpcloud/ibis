A few people have asked about this topic, so I'd like to start a discussion and
state some of my thinking about moving our SQL backends to
[sqlglot](https://github.com/tobymao/sqlglot) and away from [SQLAlchemy](https://www.sqlalchemy.org/).

Hopefully this goes without saying, but nothing here should be interpreted as
negative toward SQLAlchemy. SQLAlchemy is a wonderful project, and provides
huge value. It has served us well for years.

I think some the challenges that make SQLAlchemy difficult for us to continue
to use are **very specific to ibis**, that is, the problem of backend
cat-herding.

## Why move away from SQLAlchemy?

### Dialect sprawl

I think the primary reason to move away from SQLAlchemy is one of its greatest
strengths: pluggable dialects.

Library developers can build a SQLAlchemy dialect that natively integrates with
most of the rest of SQLAlchemy as an independent project that need not be tied
to sqlalchemy itself.

One of the challenges with this architecture with ibis is that because we have
so many backends that use SQLAlchemy we're often tied to the timelines of
whatever person or company happens to be maintaining (or not!) the dialect for
a given backend.

This is fine if you have 1 or 2 backends, but at 9 sqlalchemy backends we feel
the burden of having to deal with upstream projects' priorities.

In addition to dialects there are separate database driver packages as well,
which adds to this sprawl.

Again, I think pluggability is one of SQLAlchemy's strengths. Combining it with
ibis is what makes it challenging to continue to use.

### Other reasons

There are other reasons too, but I believe they are less important than
addressing the dialect sprawl issue.

1. sqlglot is less complex. Whether that's true after nearly two decades (the
   age of SQLAlchemy) of development remains to be seen :)
1. sqlglot has support for moving between dialects such that queries are
   semantically equivalent

## Why sqlglot?

sqlglot is a relatively new project and I think it has a lot of promise.

It's a SQL parser and transpiler with support for many of the dialects of SQL that
we care about.

Why should we move to sqlglot?

### Dialects are centralized in one place

sqlglot dialects are centralized in one place and are much easier to discover,
understand and contribute to.

Amazingly, the team working on it never seems to have any open issues and it's
not because there are no bugs! Code is reviewed quickly and bugs are fixed
extremely fast. If something isn't going to be implemented you won't be left hanging.
If something doesn't work, there's usually a workaround or extension point.

The fact that dialects are in one place addresses my main concern with
continuing to use SQLAlchemy.

### Transformations

The translation functionality in sqlglot has recently unlocked some
long-awaited features for us, like [supporting unnest for the BigQuery
backend](https://github.com/ibis-project/ibis/pull/7157) in way that is
[consistent with other
backends](https://github.com/tobymao/sqlglot/issues/2227).

These transformations ensure that backend behaviors agree with each other, even
when doing so results in extremely complex SQL.

```python
import sqlglot as sg

print(
    sg.parse_one(
        "SELECT UNNEST(x), UNNEST(y) FROM t",
        read="duckdb",
    ).sql("bigquery", pretty=True)
)
```

The output of this code is:

```sql
SELECT
  IF(pos = pos_2, col, NULL) AS col,
  IF(pos = pos_3, col_2, NULL) AS col_2
FROM t, UNNEST(GENERATE_ARRAY(0, GREATEST(ARRAY_LENGTH(x), ARRAY_LENGTH(y)) - 1)) AS pos
CROSS JOIN UNNEST(x) AS col WITH OFFSET AS pos_2
CROSS JOIN UNNEST(y) AS col_2 WITH OFFSET AS pos_3
WHERE
  (
    pos = pos_2
    OR (
      pos > (
        ARRAY_LENGTH(x) - 1
      ) AND pos_2 = (
        ARRAY_LENGTH(x) - 1
      )
    )
  )
  AND (
    pos = pos_3
    OR (
      pos > (
        ARRAY_LENGTH(y) - 1
      ) AND pos_3 = (
        ARRAY_LENGTH(y) - 1
      )
    )
  )
```

This is not a transformation that I would like to encode in ibis if we can
avoid it.

### Performance

sqlglot allows us to more easily separate expression compilation from execution.

We can choose to combine sqlglot with ADBC, or duckdb's native driver, or
whatever other thing comes along because we're not tied to a specific driver.

## Potential downsides

We've already moved the clickhouse backend to sqlglot, and are [making
improvements to it](https://github.com/ibis-project/ibis/pull/7209).

One of the key features of SQL that [ClickHouse doesn't
support](https://github.com/ClickHouse/ClickHouse/issues/6697) is correlated
subqueries.

SQLAlchemy handles some of the details of correlation for us, so we need to
port another backend that supports them to see what handling them looks like.

The way joins are represented in sqlglot is very different from sqlalchemy. We
likely to address join chaining in one way or another before moving a bunch of
our other backends to sqlglot, because sqlglot doesn't have any special
handling for this problem.

## Goals for the porting process

1. No user facing changes
1. No test changes that imply a user facing change

## Non-goals

1. "Better" SQL output (more concise, more optimized, etc)

Let's discuss!
