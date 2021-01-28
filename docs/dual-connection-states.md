`DualConnection` chooses between main and replica database connections at query time.
The choose is postponed to a query time. When the first query arrives, one of the connections
will be initialised (main or replica). There's a sequence of choices to determine which
connection should be used.

`is write context?` - it's the first and the simplest check. Some statement methods like
`executeUpdate` are intended for writes.

`is write query?` - validates SQL query.

`is replica consistent?` - Uses provided `RelicaConsistency` implementation to determine if
the replica is ready to serve consistent responses.

![DualConnection states](dual-connection-states.png "DualConnection states")

If we ended in `ReplicaConnection` or `CommitedMain` state, then the next query will go the same way.
`MainConnection` is a permanent state and `DualConnection` re-use main connection for
next queries.

## States

- `NoConnection` - when we create a new `DualConnection` it doesn't allocate
  any real database connection.
- `ReplicaConnection` - connection to the replica database has been established. It will be
  re-used until we need to switch to either `MainConnection` or `CommitedMain`.
- `MainConnection` - connection to the main database has been established and used to do
  writes to the database. `DualConnection` will keep using it until closed.
  The main reasons not to switch to replica from this state:
    - to avoid affecting transactions
    - to avoid affecting locks
- `CommitedMain` - connection to the main database has been established, but only
  for reads. The replica was temporarily inconsistent. This state shares some features
  of both `ReplicaConnection` and `MainConnection` states. It's connected to the main database,
  but can switch to the replica.


