## How DualConnection chooses between main and replica?

[DualConnection](../src/main/java/com/atlassian/db/replica/api/DualConnection.java) takes multiple aspects while deciding
which connection to use:

1. A [connection's state](dual-connection-states.md).
2. A replica's [consistency](consistency.md).
3. Context of `java.sql.Connection`/`java.sql.Statement` API usage.

Some of the methods are intended to write into the database. For example every call to `java.sq.PreparedStatement#executeUpdate`
will switch the connection's state to the main database.

4. Availability of replica
5. The query will use the main database in case it's:
    - `SELECT FOR UPDATE` statement.
    - `UPDATE` statement.
    - `DELETE` statement.
    - All calls with transaction isolation level higher than `TRANSACTION_READ_COMMITTED`.

6.  The query will use the main database in case it's an unknown function call. Known read-only functions are [standard
SQL functions](https://www.postgresql.org/docs/9.4/functions.html) and user defined functions.
