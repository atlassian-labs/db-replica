# Releasing

## Gradle
The [`gradle-release` plugin](https://bitbucket.org/atlassian/gradle-release/src/master/README.md) is used.
It documents how to release, publish and mark versions.
It can be used locally or in CI.

## GitHub Actions
GitHub Actions use Gradle to release the library.
A release build can be [triggered manually by "workflow dispatch"](trigger-gha-release.mp4).

## Current version in source
After releasing a new version:
* the [changelog](../../CHANGELOG.md) needs a new entry
* the [readme](../../README.md) installation section needs a new Maven version
* clean up internal `compatibleWithPreviousVersion` usages
