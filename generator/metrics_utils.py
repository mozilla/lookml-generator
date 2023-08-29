"""Utils for working with metric-hub."""

from typing import List, Optional, Union
from metric_config_parser.config import (
    Config,
    ConfigCollection,
    DefinitionConfig,
)
from metric_config_parser.data_source import DataSource

METRIC_HUB_REPO = "https://github.com/mozilla/metric-hub"

class MetricsConfigLoader:
    """
    Loads metric config files from an external repository.
    """

    config_collection: Optional[ConfigCollection] = None

    @property
    def configs(self) -> ConfigCollection:
        configs = getattr(self, "_configs", None)
        if configs:
            return configs

        if self.config_collection is None:
            self.config_collection = ConfigCollection.from_github_repos([METRIC_HUB_REPO])
        self._configs = self.config_collection
        return self._configs