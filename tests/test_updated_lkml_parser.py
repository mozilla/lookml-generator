import copy

import lkml

from generator.lkml_update import dump

from .utils import print_and_test


def test_parser():
    lookml = {
        "explores": [
            {
                "name": "test_explore",
                "joins": [
                    {"name": "join_a"},
                    {"name": "join_b"},
                ],
                "queries": {
                    "dimensions": ["submission_date", "app_build"],
                    "measures": ["client_count"],
                    "pivots": ["app_build"],
                    "sorts": [{"submission_date": "asc"}],
                    "name": "build_breakdown",
                },
            },
        ],
    }

    print_and_test(lookml, lkml.load(dump(copy.deepcopy(lookml))))
