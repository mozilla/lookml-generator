# lookml-generator
[![mozilla](https://circleci.com/gh/mozilla/lookml-generator.svg?style=svg)](https://circleci.com/gh/mozilla/lookml-generator/?branch=main)

*Under Active Development*

LookML Generator for Glean and Mozilla Data.

The lookml-generator has two important roles:
1. Generate a listing of all Glean/Mozilla namespaces and their associated BigQuery tables
2. From that listing, generate LookML for views, explores, and dashboards and push those to the [Look Hub project](https://github.com/mozilla/looker-hub)

## Generating Namespace Listings

At Mozilla, a namespace is a single functional area that is represented in Looker with (usually) one model*.
Each Glean application is self-contained within a single namespace, containing the data from [across that application's channels](https://probeinfo.telemetry.mozilla.org/v2/glean/app-listings).
We also support custom namespaces, which can use wildcards to denote their BigQuery datasets and tables. These are described in `custom-namespaces.yaml`.

![alt text](https://github.com/mozilla/lookml-generator/blob/main/architecture/namespaces.jpg?raw=true)

> \*  Though namespaces are not limited to a single model, we advise it for clarity's sake.

## Adding Custom Namespaces
Custom namespaces need to be defined explicitly in `custom-namespaces.yaml`. For each namespace views and explores to be generated need to be specified.

Make sure the custom namespaces is _not_ listed in `namespaces-disallowlist.yaml`.

Once changes have been approved and merged, the [lookml-generator changes can get deployed](#deploying-new-lookml-generator-changes).

## Generating LookML
Once we know which tables are associated with which namespaces, we can generate LookML files and update our Looker instance.

Lookml-generator generates LookML based on both the BigQuery schema and manual changes. For example, we would want to add `city` drill-downs for all `country` fields.
![alt text](https://github.com/mozilla/lookml-generator/blob/main/architecture/lookml.jpg?raw=true)


### Pushing Changes to Dev Branches
In addition to pushing new lookml to the [main branch](https://github.com/mozilla/looker-hub), we reset the dev branches to also
point to the commit at `main`. This only happens during production deployment runs.

To automate this process for your dev branch, add it to [this file](https://github.com/mozilla/lookml-generator/tree/main/bin/dev_branches).
You can edit that file in your browser. Open a PR and tag [data-looker](https://github.com/orgs/mozilla/teams/data-looker) for review.
You can find your dev branch by going to [Looker](https://mozilla.cloud.looker.com), entering development mode, opening the [`looker-hub`](https://mozilla.cloud.looker.com/projects/looker-hub)
project, clicking the "Git Actions" icon, and finding your personal branch in the "Current Branch" dropdown.

## Setup

Ensure Python 3.8+ is available on your machine (see [this guide](https://docs.python-guide.org/starting/install3/osx/) for instructions if you're on a mac and haven't installed anything other than the default system Python.)

You will also need the Google Cloud SDK with valid credentials.
After setting up the Google Cloud SDK, run:

```bash
gcloud config set project moz-fx-data-shared-prod
gcloud auth login --update-adc
```

Install requirements in a Python venv
```bash
python3.8 -m venv venv/
venv/bin/pip install -r requirements.txt
```

Update requirements when they change with `pip-sync`
```bash
venv/bin/pip-sync
```

Setup pre-commit hooks
```bash
venv/bin/pre-commit install
```

Run unit tests and linters
```bash
venv/bin/pytest
```

Run integration tests
```bash
venv/bin/pytest -m integration
```

Note that the integration tests require a valid login to BigQuery to succeed.

## Testing generation locally

You can test namespace generation by running:

```bash
./bin/generator namespaces
```

To generate the actual lookml (in `looker-hub`), run:

```bash
./bin/generator lookml
```

## Container Development

Most code changes will not require changes to the generation script or container.
However, you can test it locally. The following script will test generation, pushing
a new branch to the `looker-hub` repository:

```
export HUB_BRANCH_PUBLISH="yourname-generation-test-1"
export GIT_SSH_KEY_BASE64=$(cat ~/.ssh/id_rsa | base64)
make build && make run
```

## Deploying new `lookml-generator` changes

`lookml-generator` runs daily to update the `looker-hub` and `looker-spoke-default` code. Changes
to the underlying tables should automatically propogate to their respective views and explores.

However, changes to `lookml-generator` need to be tested on stage and deployed. The general process
is the following:
1. Create a PR, test on dev. It is not necessary to add Looker credentials, but the container changes
   should run using `make build && make run`, with changes reflected in LookML repos.
2. Once merged, the changes should run on stage. They will run automatically after schema deploys,
   but they can be run manually by clearing the `lookml_generator_staging` task in [Airflow](https://workflow.telemetry.mozilla.org/tree?dag_id=probe_scraper).
3. Once the changes are confirmed in stage, we first tag a new release here. Add a description with
   what the new release includes. Finally, change the Airflow variable `lookml_generator_release_str`
   to the version string you created when cutting the release. Re-run the DAG and the changes
   should take effect.
