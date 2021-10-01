# Contribution Process

The following is a set of guidelines for contributing to Dagger. These are mostly guidelines, not hard and first rules. Use your best judgment, and feel free to propose changes to this document over a pull request. Here are some important resources:

- [Concepts](../concepts/overview) section will explain to you about Dagger architecture.
- Our [roadmap](../roadmap.md) is the 10000-foot view of where we're heading in near future.
- Github [issues](https://github.com/odpf/dagger/issues) track the ongoing and reported issues.

## How can I contribute?

We use RFCs and GitHub issues to communicate ideas.

- You can report a bug, suggest a feature enhancement or can just ask questions. Reach out on Github discussions for this purpose.
- You are also welcome to add new sinks to Dagger, improve monitoring and logging and improve code quality.
- You can help with documenting new features or improve existing documentation.
- You can also review and accept other contributions if you are a maintainer.

Please submit a PR to the main branch of the Dagger repository once you are ready to submit your contribution. Code submission to Dagger \(including a submission from project maintainers\) requires review and approval from maintainers or code owners. Once the build is passed, community members will help to review the pull request and merge changes.

## Becoming a maintainer

We are always interested in adding new maintainers. What we look for is a series of contributions, good taste, and an ongoing interest in the project.

- maintainers will have write access to the Dagger repositories.
- There is no strict protocol for becoming a maintainer or PMC member. Candidates for new maintainers are typically people that are active contributors and community members.
- Candidates for new maintainers can also be suggested by current maintainers or PMC members.
- If you would like to become a maintainer, you should start contributing to Dagger in any of the ways mentioned. You might also want to talk to other maintainers and ask for their advice and guidance.

## Guidelines

Some rules need to be followed to contribute.

- Contributions can only be accepted if they contain appropriate testing \(Unit and Integration Tests\). The test coverage should not go below the original branch.
- If you are introducing a completely new feature or making any major changes to an existing one, we recommend starting with an RFC and get consensus on the basic design first.
- Make sure your local build is running with all the tests and check style passing.
- If your change is related to user-facing protocols/configurations, you need to make the corresponding change in the documentation as well.
- Docs live in the code repo under [docs](https://github.com/odpf/dagger/tree/main/docs) so that changes to that can be done in the same PR as changes to the code.

## Making a pull request

### Incorporating upstream changes from master

Our preference is the use of git rebases instead of git merge. Please sign commits

```bash
# Include -s flag to sign off
$ git commit -s -m "feat: my first commit"
```

#### Practices to follow

- Follow the [conventional commit](https://www.conventionalcommits.org/en/v1.0.0/) format for all commit messages.
- Fill in the description based on the default template configured when you first open the PR.
- Include desired labels when opening the PR.
- Add `WIP:` to PR name if more work needs to be done before review.
- Avoid force-pushing as it makes reviewing difficult.

We follow [semantic versioning](https://semver.org/) for any code change and release. The version is defined in
the [version.txt](../../../version.txt) file and is common across all the submodules.

- Bump up the patch version for every backward compatible changes like addition of new UDFs/transformers/bug fixes etc.
- In case of any contract changes bump of minor version.
- For major feature releases like upgrading the Flink version, update the major version.
