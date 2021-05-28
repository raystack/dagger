# Development Guide

The following guide will help you quickly run Dagger in your local machine. The main components of Dagger are:

* Streams: All Kafka source related information for Data consumption. 
* SQL: SQL Query to process input stream data.
* Sink: Sinking data after processing is done.

## Requirements

### Development environment

The following environment is required for Dagger development

* Java SE Development Kit 8.

### Services

The following components/services are required to develop Dagger:

* Kafka &gt; 2.4 to consume messages from.
* Corrsponding sink service to sink data to.
* Flink \(Optional\) only needed if you want to run in cluster mode. For standalone mode it's not required.

## Style Guide

### Java

We conform to the [Google Java Style Guide](https://google.github.io/styleguide/javaguide.html). Maven can helpfully take care of that for you before you commit.

## Making a pull request

#### Incorporating upstream changes from master

Our preference is the use of git rebase instead of git merge. Signing commits

```bash
# Include -s flag to signoff
$ git commit -s -m "feat: my first commit"
```

### Good practices to follow

* Follow the [conventional commit](https://www.conventionalcommits.org/en/v1.0.0/) format for all commit messages.
* Fill in the description based on the default template configured when you first open the PR.
* Include label when opening the PR.
* Add `WIP:` to PR name if more work needs to be done prior to review.
* Avoid force-pushing as it makes reviewing difficult.
