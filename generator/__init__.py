"""Generate LookML."""

import logging
import warnings

import click

from .lookml import lookml
from .namespaces import namespaces


def cli(prog_name=None):
    """Generate and run CLI."""
    commands = {"namespaces": namespaces, "lookml": lookml}

    @click.group(commands=commands)
    @click.option(
        "--log-level",
        default="WARNING",
        help="Set logging level for the python root logger",
    )
    def group(log_level):
        """CLI interface for lookml automation."""
        logging.root.setLevel(log_level)

    warnings.filterwarnings(
        "ignore",
        "Your application has authenticated using end user credentials",
        module="google.auth._default",
    )

    group(prog_name=prog_name)
