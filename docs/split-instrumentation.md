## Configurable main/replica split instrumentation

All calls to the database goes through [DualCall](../src/main/java/com/atlassian/db/replica/spi/DualCall.java).
By default [DualConnection](../src/main/java/com/atlassian/db/replica/api/DualConnection.java) forwards all the calls.
It can be used to log queries, gather metrics or to handle exceptions.

![Split](split-instrumentation.png "SplitInstrumentation")

Every database operation on replica will go through `DualCall#callReplica`.
Every call to the main database will go through `DualCall#callMain` method.
