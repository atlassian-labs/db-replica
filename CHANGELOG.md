# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/).
This project **DOES NOT** adhere to [Semantic Versioning](http://semver.org/spec/v2.0.0.html) yet.

## [Unreleased]
[Unreleased]: https://bitbucket.org/atlassian/db-replica/branches/compare/master%0Drelease-0.1.0

## [0.1.0] - 2020-11-18
[0.1.0]: https://bitbucket.org/atlassian/db-replica/branches/compare/release-0.1.0%0Dinitial-commit

### Added
- Add `api.DualConnection`, `api.SqlConnection`
- Add `spi.DualConnectionOperation`, `spi.ReplicaConsistency`, `spi.ConnectionProvider`
- Add `impl.ClockReplicaConsistency`, `impl.LsnReplicaConsistency`
- Add `impl.ForwardConnectionOperation`, `impl.PrintfDualConnectionOperation`
