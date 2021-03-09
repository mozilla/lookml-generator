"""Generate LookML."""

import warnings

import click

from .lookml import lookml
from .namespaces import namespaces


def cli(prog_name=None):
    """Generate and run CLI."""
    commands = {"namespaces": namespaces, "lookml": lookml}

    @click.group(commands=commands)
    def group():
        """CLI interface for lookml automation."""

    warnings.filterwarnings(
        "ignore",
        "Your application has authenticated using end user credentials",
        module="google.auth._default",
    )

    group(prog_name=prog_name)
