# Releasing

## Releasing new version od db-replica
* A new changed merged in source
* Release and publish a new version of library 
* Mark release with the new version containing:
  * the [changelog](../../CHANGELOG.md) needs a new entry
  * the [readme](../../README.md) installation section needs a new Maven version
  * (optional) clean up internal `compatibleWithPreviousVersion` usages


### Gradle
The [`gradle-release` plugin](https://bitbucket.org/atlassian/gradle-release/src/master/README.md) is used.
It documents how to release, publish and mark versions.
It can be used locally or in CI.

### GitHub Actions
GitHub Actions use Gradle to release the library.
A release build can be [triggered manually by "workflow dispatch"](trigger-gha-release.mp4).

