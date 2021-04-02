# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/).
This project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## API
The API consists of all public Java types from `com.atlassian.db.replica.api`, `com.atlassian.db.replica.spi` and their subpackages:

  * [source compatibility]
  * [binary compatibility]
  * [behavioral compatibility] with behavioral contracts expressed via Javadoc

[source compatibility]: http://cr.openjdk.java.net/~darcy/OpenJdkDevGuide/OpenJdkDevelopersGuide.v0.777.html#source_compatibility
[binary compatibility]: http://cr.openjdk.java.net/~darcy/OpenJdkDevGuide/OpenJdkDevelopersGuide.v0.777.html#binary_compatibility
[behavioral compatibility]: http://cr.openjdk.java.net/~darcy/OpenJdkDevGuide/OpenJdkDevelopersGuide.v0.777.html#behavioral_compatibility

### POM
Changing the license is breaking a contract.
Adding a requirement of a major version of a dependency is breaking a contract.
Dropping a requirement of a major version of a dependency is a new contract.

## [Unreleased]
[Unreleased]: https://github.com/atlassian-labs/db-replica/compare/release-1.2.0...master

### Fixed
- Implemented missing methods in `DualConnection`

## [1.2.0] - 2021-04-01
[1.2.0]: https://github.com/atlassian-labs/db-replica/compare/release-1.1.0...release-1.2.0

### Added
- `ThrottledCache`
- `SuppliedCache<T>`

## [1.1.0] - 2021-03-18
[1.1.0]: https://github.com/atlassian-labs/db-replica/compare/release-1.0.0...release-1.1.0

### Added
- `AuroraPostgresLsnReplicaConsistency`

## [1.0.0] - 2021-03-15
[1.0.0]: https://github.com/atlassian-labs/db-replica/compare/release-0.1.30...release-1.0.0

### Removed
- `DualConnection.Builder.stateListener()`
- `StateListener`
- `State`
- `DualConnection.Builder.circuitBreaker()`
- `CircuitBreaker`
- `BreakerState`

## [0.1.31] - 2021-03-10
[0.1.31]: https://github.com/atlassian-labs/db-replica/compare/release-0.1.30...release-0.1.31

### Added
-`RouteDecision#mustRunOnMain()` and `RouteDecision#willRunOnMain()`

### Fixed
- clarified read/write Reasons

### Removed
- `Reason.isWrite()`

## [0.1.30] - 2021-03-09
[0.1.30]: https://github.com/atlassian-labs/db-replica/compare/release-0.1.29...release-0.1.30

### Added
- Add support for compatibility mode

### Fixed
- `Connection#nativeSQL` uses replica database

## [0.1.29] - 2021-03-01
[0.1.29]: https://github.com/atlassian-labs/db-replica/compare/release-0.1.28...release-0.1.29

### Fixed
- `Connection#close` can cause an exception.

## [0.1.28] - 2021-02-26
[0.1.28]: https://github.com/atlassian-labs/db-replica/compare/release-0.1.27...release-0.1.28

### Fixed
- Initialize connection once
- Propagate read-only mode immediately

## [0.1.27] - 2021-02-23
[0.1.27]: https://github.com/atlassian-labs/db-replica/compare/release-0.1.26...release-0.1.27

### Fixed
- Propagate the read-only mode to the main connection
- Reset the read-only mode before releasing a connection

## [0.1.26] - 2021-02-18
[0.1.26]: https://github.com/atlassian-labs/db-replica/compare/release-0.1.25...release-0.1.26

### Fixed
- Set connection properties while switching to replica
- Exclude [Sequence Manipulation Functions](https://www.postgresql.org/docs/9.4/functions-sequence.html) from read-only functions

## [0.1.25] - 2021-02-18
[0.1.25]: https://github.com/atlassian-labs/db-replica/compare/release-0.1.24...release-0.1.25

### Added
- Add support for read-only functions

### Fixed
- Avoid unnecessary switch to `MainConnection` while setting `read-only` mode
- Statement Behavior changes don't affect DualConnection's state

## [0.1.24] - 2021-02-16
[0.1.24]: https://github.com/atlassian-labs/db-replica/compare/release-0.1.23...release-0.1.24

### Added
- Add `COMMITED_MAIN` state
- Add `DualConnection#Builder.databaseCall`
- Add `spi.DatabaseCall`
- Add `RouteDecision` and `Reason` for per query visibility

### Removed
- `spi.DualCall`
- `DualConnection#Builder.dualCall`

## [0.1.23] - 2021-02-09
[0.1.23]: https://github.com/atlassian-labs/db-replica/compare/release-0.1.22...release-0.1.23

### Added
- Add `State#getName` method.

## [0.1.22] - 2021-02-05
[0.1.22]: https://github.com/atlassian-labs/db-replica/compare/release-0.1.21...release-0.1.22

### Added
- Add `PessimisticPropagationConsistency.Builder`.
- Add `StateListener`.

### Removed
- Remove `ReplicaConsistency.assumePropagationDelay`. Use `PessimisticPropagationConsistency.Builder` instead.

## [0.1.21] - 2021-01-08
[0.1.21]: https://github.com/atlassian-labs/db-replica/compare/release-0.1.20...release-0.1.21

No changes.

## [0.1.20] - 2021-01-08
[0.1.20]: https://github.com/atlassian-labs/db-replica/compare/release-0.1.19...release-0.1.20

### Fixed
- Propagate `Consistency#write` when the same statement used first with `executeQuery` and then with `executeUpdate`.

## [0.1.19] - 2021-01-05
[0.1.19]: https://github.com/atlassian-labs/db-replica/compare/release-0.1.18...release-0.1.19

### Fixed
- Avoid unnecessary consistency writes.

## [0.1.18] - 2020-12-17
[0.1.18]: https://github.com/atlassian-labs/db-replica/compare/release-0.1.17...release-0.1.18

### Fixed
- Don't assume inconsistency forever, when no writes happen.

## [0.1.17] - 2020-12-16
[0.1.17]: https://github.com/atlassian-labs/db-replica/compare/release-0.1.16...release-0.1.17

### Changed
- `spi.ConnectionProvider#getMainConnection()` throws `SQLException`
- `spi.ConnectionProvider#getReplicaConnection()` throws `SQLException`

### Fixed
- Complex queries wrongly run on the main database

## [0.1.16] - 2020-12-16
[0.1.16]: https://github.com/atlassian-labs/db-replica/compare/release-0.1.15...release-0.1.16

### Fixed
- `DualCall` calls main when the replica is not consistent.

## [0.1.15] - 2020-12-07
[0.1.15]: https://github.com/atlassian-labs/db-replica/compare/release-0.1.14...release-0.1.15

### Fixed
- NPE when calling `DualConnection#isReadOnly`

## [0.1.14] - 2020-12-07
[0.1.14]: https://github.com/atlassian-labs/db-replica/compare/release-0.1.13...release-0.1.14

### Changed
- `spi.ReplicaConsistency#isConsistent(Connection replica)` to `spi.ReplicaConsistency#isConsistent(Supplier<Connection> replica)`

### Fixed
- Avoid fetching replica connections when not needed.

## [0.1.13] - 2020-12-02
[0.1.13]: https://github.com/atlassian-labs/db-replica/compare/release-0.1.12...release-0.1.13

### Fixed
- Release connection's reference on close
- Keep `close` related contract in `DualConnection` API
- Make `close` safe
- Release `Statement` when closed
- Keep `close` related contract in `Statement` API
- Keep `close` related contract in `PreparedStatement` API

## [0.1.12] - 2020-11-27
[0.1.12]: https://github.com/atlassian-labs/db-replica/compare/release-0.1.11...release-0.1.12

### Added
- Inject `Cache` to allow multiple ways of holding the "last write to main".
- Add `ReplicaConsistency.assumePropagationDelay`.
- Add `Cache.assumePropagationDelay.cacheMonotonicValuesInMemory`.

## [0.1.11] - 2020-11-27
[0.1.11]: https://github.com/atlassian-labs/db-replica/compare/release-0.1.10...release-0.1.11

### Fixed
- implementation of `Connection#setSavepoint`
- implementation of `Connection#rollback(Savepoint savepoint)`
- implementation of `Connection#releaseSavepoint(Savepoint savepoint)`

## [0.1.10] - 2020-11-26
[0.1.10]: https://github.com/atlassian-labs/db-replica/compare/release-0.1.9...release-0.1.10

### Fixed
- Handle multiple calls for `setReadOnly`

## [0.1.9] - 2020-11-25
[0.1.9]: https://github.com/atlassian-labs/db-replica/compare/release-0.1.8...release-0.1.9

### Fixed
- Assign deletes to the main connection for `executeQuery` calls
- Keep using the main connection
- `setReadOnly` determines connection

### Removed
- `api.circuitbreaker.DualConnectionException`

### Changed
- throw original exception instead of `DualConnectionException`

## [0.1.8] - 2020-11-25
[0.1.8]: https://github.com/atlassian-labs/db-replica/compare/release-0.1.7...release-0.1.8

### Fixed
- Hiding `Connection#close` failure
- Assign updates to the main connection for `executeQuery` calls

## [0.1.7] - 2020-11-25
[0.1.7]: https://github.com/atlassian-labs/db-replica/compare/release-0.1.6...release-0.1.7

### Fixed
- implementation of `Statement#setEscapeProcessing`
- implementation of `Statement#setMaxRows`
- implementation of `Statement#setMaxFieldSize`
- implementation of `Statement#setFetchDirection`
- implementation of `Statement#setPoolable`
- implementation of `Statement#setLargeMaxRows`

### Removed
- dependency on `jcip-annotations`
- dependency on `postgresql`
- dependency on `commons-lang3`
- dependency on `atlassian-util-concurrent`
- `impl.LsnReplicaConsistency`

## [0.1.6] - 2020-11-23
[0.1.6]: https://github.com/atlassian-labs/db-replica/compare/release-0.1.5...release-0.1.6

### Added
- Add `spi.circuitbreaker.CircuitBreaker`

## [0.1.5] - 2020-11-23
[0.1.5]: https://github.com/atlassian-labs/db-replica/compare/release-0.1.4...release-0.1.5

### Added
- Support for single connection provider

## [0.1.4] - 2020-11-20
[0.1.4]: https://github.com/atlassian-labs/db-replica/compare/release-0.1.3...release-0.1.4

### Changed
- Use `net.jcip:jcip-annotations:1.0` instead of `com.github.stephenc`

## [0.1.3] - 2020-11-20
[0.1.3]: https://github.com/atlassian-labs/db-replica/compare/release-0.1.2...release-0.1.3

### Fixed
- implementation of `Connection#isWrapperFor`
- implementation of `Connection#unwrap`
- implementation of `Connection#getSchema`
- implementation of `Connection#createArrayOf`
- implementation of `Connection#isValid`
- implementation of `Connection#getHoldability`
- implementation of `Connection#setHoldability`
- implementation of `Connection#getTypeMap`
- implementation of `Connection#setTypeMap`
- implementation of `Connection#getCatalog`
- implementation of `Connection#setCatalog`
- implementation of `Connection#getWarnings`
- implementation of `Connection#clearWarnings`
- implementation of `Connection#getTransactionIsolation`
- implementation of `Statemtent#unwrap`
- implementation of `Statemtent#isWrapperFor`
- implementation of `Statemtent#clearBatch`
- implementation of `Statemtent#addBatch`
- implementation of `Statemtent#getMoreResults`
- implementation of `Statemtent#getUpdateCount`
- implementation of `Statemtent#getResultSet`
- implementation of `Statemtent#getWarnings`
- implementation of `Statemtent#clearWarnings`

- NPE in `ReplicaStatement#isClosed`

## [0.1.2] - 2020-11-18
[0.1.2]: https://github.com/atlassian-labs/db-replica/compare/release-0.1.1...release-0.1.2

### Changed
- Renamed:
    - `api.SqlConnection` to `api.SqlCall`
    - `spi.DualConnectionOperation` to `spi.DualCall`
    - `impl.ForwardConnectionOperation` to `impl.ForwardCall`
    - `impl.PrintfDualConnectionOperation` to `impl.TimeRatioPrinter`

## [0.1.0] - 2020-11-18
[0.1.0]: https://github.com/atlassian-labs/db-replica/compare/initial-commit...release-0.1.0

### Added
- Add `api.DualConnection`, `api.SqlConnection`
- Add `spi.DualConnectionOperation`, `spi.ReplicaConsistency`, `spi.ConnectionProvider`
- Add `impl.ClockReplicaConsistency`, `impl.LsnReplicaConsistency`
- Add `impl.ForwardConnectionOperation`, `impl.PrintfDualConnectionOperation`
