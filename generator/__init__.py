"""Generate LookML."""

import click

from .namespaces import namespaces


def cli(prog_name=None):
    """Generate and run CLI."""
    commands = {"namespaces": namespaces}

    @click.group(commands=commands)
    def group():
        """CLI interface for lookml automation."""

    group(prog_name=prog_name)
