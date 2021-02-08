# lookml-generator
*Under Active Development*

LookML Generator for Glean and Mozilla Data.

The lookml-generator has two important roles:
1. Generate a listing of all Glean/Mozilla namespaces and their associated BigQuery tables
2. From that listing, generate LookML for views, explores, and dashbaords and push those to the [Look Hub project](https://github.com/mozilla/looker-hub)

## Generating Namespace Listings

At Mozilla, a namespace is a single functional area that is represented in Looker with (usually) one model*.
Each Glean application is self-contained within a single namespace, containing the data from [across that application's channels](https://probeinfo.telemetry.mozilla.org/v2/glean/app-listings).
We also support custom namespaces, which can use wildcards to denote their BigQuery datasets and tables. These are described in `custom-namespaces.yaml`.

![alt text](https://github.com/mozilla/lookml-generator/blob/main/architecture/namespaces.jpg?raw=true)

* Though namespaces are not limited to a single model, we advise it for clarity's sake.

## Generating LookML
Once we know which tables are associated with which namespaces, we can generate LookML files and update our Looker instance.

Lookml-generator generates LookML based on both the BigQuery schema and manual changes. For example, we would want to add `city` drill-downs for all `country` fields.
![alt text](https://github.com/mozilla/lookml-generator/blob/main/architecture/lookml.jpg?raw=true)
