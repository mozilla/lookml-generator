"""Generate LookML.

.. include:: ../README.md
.. include:: ../architecture/namespaces_yaml.md
"""

__docformat__ = "restructuredtext"

import sys
import warnings

import click
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import bigquery

from .lookml import lookml
from .namespaces import namespaces
from .spoke import update_spoke


def is_authenticated():
    """Check if the user is authenticated to GCP."""
    try:
        bigquery.Client()
    except DefaultCredentialsError:
        return False
    return True


def cli(prog_name=None):
    """Generate and run CLI."""
    if not is_authenticated():
        print(
            "Authentication to GCP required. Run `gcloud auth login --update-adc` "
            "and check that the project is set correctly."
        )
        sys.exit(1)

    commands = {
        "namespaces": namespaces,
        "lookml": lookml,
        "update-spoke": update_spoke,
    }

    @click.group(commands=commands)
    def group():
        """CLI interface for lookml automation."""

    warnings.filterwarnings(
        "ignore",
        "Your application has authenticated using end user credentials",
        module="google.auth._default",
    )

    group(prog_name=prog_name)
