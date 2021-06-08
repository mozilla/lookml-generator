"""Generate LookML.

.. include:: ../README.md
.. include:: ../architecture/namespaces_yaml.md
"""
__docformat__ = "restructuredtext"

import warnings

import click

from .content import generate_content
from .lookml import lookml
from .namespaces import namespaces
from .spoke import update_spoke


def cli(prog_name=None):
    """Generate and run CLI."""
    commands = {
        "namespaces": namespaces,
        "lookml": lookml,
        "update-spoke": update_spoke,
        "content": generate_content,
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
