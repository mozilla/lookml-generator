# lookml-generator
[![mozilla](https://circleci.com/gh/mozilla/lookml-generator.svg?style=svg)](https://circleci.com/gh/mozilla/lookml-generator/?branch=main)

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

Ensure Python 3.11+ is available on your machine (see [this guide](https://docs.python-guide.org/starting/install3/osx/) for instructions if you're on a mac and haven't installed anything other than the default system Python.)

You will also need the Google Cloud SDK with valid credentials.
After setting up the Google Cloud SDK, run:

```bash
gcloud config set project moz-fx-data-shared-prod
gcloud auth login --update-adc
```

Install requirements in a Python venv
```bash
python3.11 -m venv venv/
venv/bin/python -m pip install --no-deps -r requirements.txt
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

Airflow updates the two repositories [each morning](https://github.com/mozilla/telemetry-airflow/blob/main/dags/probe_scraper.py#L320).
If you need your changes deployed quickly, wait for the container to build after you merge to
`main`, and re-run the task in Airflow (`lookml_generator`, in the `probe_scraper` DAG).

## `generate` Command Explained - High Level Explanation

When `make run` is executed a Docker container is spun up using the latest `lookml-generator` Docker image on your machine and runs the [`generate` script](bin/generate) using configuration defined at the top of the script unless [overridden using environment variables](./docker-compose.yml#L13-L25) (see the [Container Development](#container-development) section above).

Next, the process authenticates with GitHub, clones the [`looker-hub` repository](https://github.com/mozilla/looker-hub), and creates the branch defined in the `HUB_BRANCH_PUBLISH` config variable both locally and in the remote. Then it proceeds to checkout into the looker-hub `base` branch and pulls it from the remote.

Once the setup is done, the process generates `namespaces.yaml` and uses it to generate LookML code. A git diff is executed to ensure that the files that already exist in the `base` branch are not being modified. If changes are detected then the process exists with an error code. Otherwise, it proceeds to create a commit and push it to the remote dev branch created earlier.

When following the `Container Development` steps, the entire process results in a dev branch in `looker-hub` with brand new generated LookML code which can be tested by going to Looker, switching to the "development mode" and selecting the dev branch just created/updated by this command. This will result in Looker using the brand new LookML code just generated. Otherwise, changes merged into `main` in this repo will become available on looker-hub `main` when the scheduled Airflow job runs.
