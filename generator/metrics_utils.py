"""Utils for working with metric-hub."""

from typing import List, Optional

from metric_config_parser.config import ConfigCollection
from metric_config_parser.metric import MetricDefinition

METRIC_HUB_REPO = "https://github.com/mozilla/metric-hub"


class _MetricsConfigLoader:
    """Loads metric config files from an external repository."""

    config_collection: Optional[ConfigCollection] = None
    repos: List[str] = [METRIC_HUB_REPO]

    @property
    def configs(self) -> ConfigCollection:
        configs = getattr(self, "_configs", None)
        if configs:
            return configs

        if self.config_collection is None:
            self.config_collection = ConfigCollection.from_github_repos(self.repos)
        self._configs = self.config_collection
        return self._configs

    def update_repos(self, repos: List[str]):
        """Change the repos to load configs from."""
        self.repos = repos
        self.config_collection = ConfigCollection.from_github_repos(repos)

    def metrics_of_data_source(
        self, data_source: str, namespace: str
    ) -> List[MetricDefinition]:
        """Get the metric definitions that use a specific data source."""
        metrics = []
        for definition in self.configs.definitions:
            if definition.platform == namespace:
                for _, metric_definition in definition.spec.metrics.definitions.items():
                    if (
                        metric_definition.data_source
                        and metric_definition.data_source.name == data_source
                    ):
                        metrics.append(metric_definition)

        return metrics

    def data_sources_of_namespace(self, namespace: str) -> List[str]:
        """
        Get the data source slugs in the specified namespace.

        Filter out data sources that are unused.
        """
        data_sources = []
        for definition in self.configs.definitions:
            for data_source_slug in definition.spec.data_sources.definitions.keys():
                if (
                    definition.platform == namespace
                    and len(
                        MetricsConfigLoader.metrics_of_data_source(
                            data_source_slug, definition.platform
                        )
                    )
                    > 0
                ):
                    data_sources.append(data_source_slug)

        return data_sources


MetricsConfigLoader = _MetricsConfigLoader()
