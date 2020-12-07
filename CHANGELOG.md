# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/).
This project **DOES NOT** adhere to [Semantic Versioning](http://semver.org/spec/v2.0.0.html) yet.

## [Unreleased]
[Unreleased]: https://bitbucket.org/atlassian/db-replica/branches/compare/master%0Drelease-0.1.14

### Fixed
- NPE when calling `DualConnection#isReadOnly`

## [0.1.14] - 2020-12-07
[0.1.14]: https://bitbucket.org/atlassian/db-replica/branches/compare/release-0.1.14%0Drelease-0.1.13

### Changed
- `spi.ReplicaConsistency#isConsistent(Connection replica)` to `spi.ReplicaConsistency#isConsistent(Supplier<Connection> replica)`

### Fixed
- Avoid fetching replica connections when not needed.

## [0.1.13] - 2020-12-02
[0.1.13]: https://bitbucket.org/atlassian/db-replica/branches/compare/release-0.1.13%0Drelease-0.1.12

### Fixed
- Release connection's reference on close
- Keep `close` related contract in `DualConnection` API
- Make `close` safe
- Release `Statement` when closed
- Keep `close` related contract in `Statement` API
- Keep `close` related contract in `PreparedStatement` API

## [0.1.12] - 2020-11-27
[0.1.12]: https://bitbucket.org/atlassian/db-replica/branches/compare/release-0.1.12%0Drelease-0.1.11

### Added
- Inject `Cache` to allow multiple ways of holding the "last write to main".
- Add `ReplicaConsistency.assumePropagationDelay`.
- Add `Cache.assumePropagationDelay.cacheMonotonicValuesInMemory`.

## [0.1.11] - 2020-11-27
[0.1.11]: https://bitbucket.org/atlassian/db-replica/branches/compare/release-0.1.11%0Drelease-0.1.10

### Fixed
- implementation of `Connection#setSavepoint`
- implementation of `Connection#rollback(Savepoint savepoint)`
- implementation of `Connection#releaseSavepoint(Savepoint savepoint)`

## [0.1.10] - 2020-11-26
[0.1.10]: https://bitbucket.org/atlassian/db-replica/branches/compare/release-0.1.10%0Drelease-0.1.9

### Fixed
- Handle multiple calls for `setReadOnly`

## [0.1.9] - 2020-11-25
[0.1.9]: https://bitbucket.org/atlassian/db-replica/branches/compare/release-0.1.9%0Drelease-0.1.8

### Fixed
- Assign deletes to the main connection for `executeQuery` calls
- Keep using the main connection
- `setReadOnly` determines connection

### Removed
- `api.circuitbreaker.DualConnectionException`

### Changed
- throw original exception instead of `DualConnectionException`

## [0.1.8] - 2020-11-25
[0.1.8]: https://bitbucket.org/atlassian/db-replica/branches/compare/release-0.1.8%0Drelease-0.1.7

### Fixed
- Hiding `Connection#close` failure
- Assign updates to the main connection for `executeQuery` calls

## [0.1.7] - 2020-11-25
[0.1.7]: https://bitbucket.org/atlassian/db-replica/branches/compare/release-0.1.7%0Drelease-0.1.6

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
[0.1.6]: https://bitbucket.org/atlassian/db-replica/branches/compare/release-0.1.6%0Drelease-0.1.5

### Added
- Add `spi.circuitbreaker.CircuitBreaker`

## [0.1.5] - 2020-11-23
[0.1.5]: https://bitbucket.org/atlassian/db-replica/branches/compare/release-0.1.5%0Drelease-0.1.4

### Added
- Support for single connection provider

## [0.1.4] - 2020-11-20
[0.1.4]: https://bitbucket.org/atlassian/db-replica/branches/compare/release-0.1.4%0Drelease-0.1.3

### Changed
- Use `net.jcip:jcip-annotations:1.0` instead of `com.github.stephenc`

## [0.1.3] - 2020-11-20
[0.1.3]: https://bitbucket.org/atlassian/db-replica/branches/compare/release-0.1.3%0Drelease-0.1.2

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
[0.1.2]: https://bitbucket.org/atlassian/db-replica/branches/compare/release-0.1.2%0Drelease-0.1.1

### Changed
- Renamed:
    - `api.SqlConnection` to `api.SqlCall`
    - `spi.DualConnectionOperation` to `spi.DualCall`
    - `impl.ForwardConnectionOperation` to `impl.ForwardCall`
    - `impl.PrintfDualConnectionOperation` to `impl.TimeRatioPrinter`

## [0.1.0] - 2020-11-18
[0.1.0]: https://bitbucket.org/atlassian/db-replica/branches/compare/release-0.1.0%0Dinitial-commit

### Added
- Add `api.DualConnection`, `api.SqlConnection`
- Add `spi.DualConnectionOperation`, `spi.ReplicaConsistency`, `spi.ConnectionProvider`
- Add `impl.ClockReplicaConsistency`, `impl.LsnReplicaConsistency`
- Add `impl.ForwardConnectionOperation`, `impl.PrintfDualConnectionOperation`
