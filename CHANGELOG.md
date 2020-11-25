# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/).
This project **DOES NOT** adhere to [Semantic Versioning](http://semver.org/spec/v2.0.0.html) yet.

## [Unreleased]
[Unreleased]: https://bitbucket.org/atlassian/db-replica/branches/compare/master%0Drelease-0.1.6

### Fixed
- implementation of `Statement#setEscapeProcessing`

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
