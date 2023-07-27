# Releasing

1. Release and publish a new version of the library:
    * Perform the release using the [GitHub Actions CI workflow](#performing-a-release) by following the steps outlined in the "Performing a Release" section of this document.
    * If you want to debug it, you can run it locally as per [`gradle-release` plugin] docs.
2. Update the repo to the new version (via pull request):
    * Cut off the `Unreleased` section in the [changelog] with the new version.
    * Update the version in the [readme] installation section.
    * Clean up internal `compatibleWithPreviousVersion` usages if they exist.

## Performing a Release

1. Go to the "Actions" tab in your GitHub repository.
2. Click on the "CI" workflow.
3. Click on "Run workflow" in the top right corner.
4. Select the branch you want to release from the dropdown list.
5. In the "Release?" input field, enter the type of release (Major/Minor/Patch/None). If you don't want to create a release, enter 'None'.
6. Optionally, in the "Prerelease?" input field, enter the prerelease stage (Alpha/Beta/RC/None). If you don't want to create a prerelease, enter 'None'.
7. Click on the "Run workflow" button.

## Release Types
- Major: A major release indicates significant changes or features that may break backward compatibility.
- Minor: A minor release includes new features or improvements that are backward compatible.
- Patch: A patch release contains bug fixes that are backward compatible.

## Prerelease Stages
- Alpha/Beta: An alpha/beta release is an early version of the code that may have incomplete features or known issues. It is intended for testing and feedback purposes.
- RC (Release Candidate): A release candidate is a version of the software that is considered feature-complete and has undergone testing. It is intended for final testing before the release.

[changelog]: ../../CHANGELOG.md
[readme]: ../../README.md
[`gradle-release` plugin]: https://bitbucket.org/atlassian/gradle-release/src/master/README.md
[triggering a release]: trigger-gha-release.md
