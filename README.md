# db-replica

[![license][license img]](LICENSE) [![PRs Welcome][contrib img]](CONTRIBUTING.md)

[license img]: https://img.shields.io/badge/license-Apache%202.0-blue.svg?style=flat-square
[contrib img]: https://img.shields.io/badge/PRs-welcome-brightgreen.svg?style=flat-square

Using database replicas unlocks horizontal scalability. Some replica designs force replicas to be read-only.
Read-only queries can be sent to the replica, while others have to go to the main DB.
The `db-replica` API automatically routes the queries to the correct node.
It integrates at the `java.sql.Connection` level, so you don't have to hunt down hundreds of queries manually.

## Usage

```java
import com.atlassian.db.replica.api.*;
import java.sql.*;
import java.time.*;

class Example {

    private final ReplicaConsistency consistency = ReplicaConsistency.assumePropagationDelay(
        Duration.ofMillis(100),
        Clock.systemUTC(),
        Cache.cacheMonotonicValuesInMemory()
    );

    ResultSet queryReplicaOrMain(String sql) {
        try (ConnectionProvider connectionProvider = new PostgresConnectionProvider()) {
            Connection connection = DualConnection.builder(connectionProvider, consistency).build();
            return connection.prepareStatement(sql).executeQuery();
        }
    }
}
```

## Installation

Maven:
```xml
<dependency>
    <groupId>com.atlassian.db.replica</groupId>
    <artifactId>db-replica</artifactId>
    <version>0.1.20</version>
</dependency>
```

## Documentation

See Javadoc of classes in the `api` and `spi` packages.
See [DualConnection states UML](docs/dual-connection-states.md).

## Tests

Run all checks: `./gradlew build`
Run just the unit tests: `./gradlew test`

## Contributions

Please see [CONTRIBUTING.md](CONTRIBUTING.md) for details.

## License

Apache 2.0 licensed, see [LICENSE](LICENSE) file.

[![With ❤️ from Atlassian][cheers img]](https://www.atlassian.com)

[cheers img]: https://raw.githubusercontent.com/atlassian-internal/oss-assets/master/banner-cheers-light.png
