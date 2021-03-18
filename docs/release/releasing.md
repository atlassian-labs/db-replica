# Releasing
1. Ensure that the next version maintains SemVer:
    * Check if `Unreleased` changes need a MINOR or MAJOR bump.
    * If yes, then mark the next version as per [`gradle-release` plugin] docs.
    * Otherwise, it will default to a PATCH bump.
2. Release and publish a new version of library:
    * It can be [triggered in GitHub Actions].
    * If you want to debug it, you can run it locally as per [`gradle-release` plugin] docs.
3. Update the repo to the new version (via pull request):
    * Cut off the `Unreleased` section in the [changelog] with the new version.
    * Update the version in the [readme] installation section.
    * Clean up internal `compatibleWithPreviousVersion` usages if they exist.

[changelog]: ../../CHANGELOG.md
[readme]: ../../README.md
[`gradle-release` plugin]: https://bitbucket.org/atlassian/gradle-release/src/master/README.md
[triggered in GitHub Actions]: trigger-gha-release.mp4
