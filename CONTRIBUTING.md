<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**

- [How to Contribute](#how-to-contribute)
  - [Contributing UDFs (User Defined Functions)](#contributing-udfs-user-defined-functions)
  - [Contributor License Agreement](#contributor-license-agreement)
  - [Code reviews](#code-reviews)
  - [Community Guidelines](#community-guidelines)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# How to Contribute

We'd love to accept your patches and contributions to this project. There are
just a few small guidelines you need to follow.

# Developement Environment Setup

### Python

The following section lists the tools you need to install to work on this project.

Install Python with pyenv.
you can install pyenv with `curl https://pyenv.run | bash`

```shell script
PYTHON_VERSION=3.8.5
pyenv install $PYTHON_VERSION
pyenv virtualenv $PYTHON_VERISON bigquery-utils
pyenv activte bigquery-utils

pip3 install -r requirements-dev.txt
```

### Terraform

The following section lists the tools you need to install if you want to use Terraform:

1. Install [Terraform v0.12.*](https://www.terraform.io/downloads.html) - version above v0.12.28 are tested
1. Install [terragrunt v0.23.31](https://github.com/gruntwork-io/terragrunt/releases/tag/v0.23.31)
1. Install [gcloud](https://cloud.google.com/sdk/docs/install) tool - To do it, run: ``curl https://sdk.cloud.google.com | bash``

#### Pre-commit Dependencies

The following section lists the tools you need to install if you want to use [Pre-commit](#pre-commit) checks:

1. Install [Docker](https://docs.docker.com/get-docker/):

    - For Linux, run: `curl https://get.docker.com | bash`
    - For Max OS/Windows, follow guide: [Get docker](https://docs.docker.com/get-docker/)

1. Install [shellcheck](https://github.com/koalaman/shellcheck)
1. Install [tflint](https://github.com/terraform-linters/tflint)
1. Install [terraform-docs](https://github.com/terraform-docs/terraform-docs#installation)
1. Install [pre-commit](https://pre-commit.com/#installation) (included in requirements-dev.txt

It is strongly recommended but not required to contribute to this repository.
You can also rely on checks run on CI but this will slow down your development cycles.

## Pre-commit

The project heavily uses pre-commits checks which perform static checks.
Those pre-commits are configured in the
`.pre-commit-config.yaml` file and they can be locally installed with pre-commit python
package - specified as a dependency in the requirements-dev.txt.
You can check [Pre-commit documentation](https://pre-commit.com/) how to use pre-commit.

Below just a few basic commands:

The easiest way is to install pre-commit using the requirements and run:

```shell script
pre-commit install
```

This way all the checks will be automatically executed on changed files when you attempt
to run `git commit`. You can also run checks on all staged files by running:

```shell script
pre-commit run
```

You can also run checks for all files by running:

```shell script
pre-commit run --all-files
```

See [Pre-commit documentation](https://pre-commit.com/) for more usage examples.



## Contributing UDFs (User Defined Functions)
If you'd like to contribute UDFs to this repository, please see the
[contributing instructions](/udfs/CONTRIBUTING.md) for UDFs to get started.

## Contributor License Agreement

Contributions to this project must be accompanied by a Contributor License
Agreement. You (or your employer) retain the copyright to your contribution;
this simply gives us permission to use and redistribute your contributions as
part of the project. Head over to <https://cla.developers.google.com/> to see
your current agreements on file or to sign a new one.

You generally only need to submit a CLA once, so if you've already submitted one
(even if it was for a different project), you probably don't need to do it
again.

## Code reviews

All submissions, including submissions by project members, require review. We
use GitHub pull requests for this purpose. Consult
[GitHub Help](https://help.github.com/articles/about-pull-requests/) for more
information on using pull requests.

## Community Guidelines

This project follows
[Google's Open Source Community Guidelines](https://opensource.google.com/conduct/).
