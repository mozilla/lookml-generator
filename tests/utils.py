"""Utility functions for tests."""

import pprint
from typing import List

from google.cloud import bigquery
from google.cloud.bigquery.schema import SchemaField


def get_differences(expected, result, path="", sep="."):
    """
    Get the differences between two JSON-like python objects.

    For complicated objects, this is a big improvement over pytest -vv.
    """
    differences = []

    if expected is not None and result is None:
        differences.append(("Expected exists but not Result", path))
    if expected is None and result is not None:
        differences.append(("Result exists but not Expected", path))
    if expected is None and result is None:
        return differences

    exp_is_dict, res_is_dict = isinstance(expected, dict), isinstance(result, dict)
    exp_is_list, res_is_list = isinstance(expected, list), isinstance(result, list)
    if exp_is_dict and not res_is_dict:
        differences.append(("Expected is dict but not Result", path))
    elif res_is_dict and not exp_is_dict:
        differences.append(("Result is dict but not Expected", path))
    elif not exp_is_dict and not res_is_dict:
        if exp_is_list and res_is_list:
            for i in range(max(len(expected), len(result))):
                if i >= len(result):
                    differences.append(
                        (f"Result missing element {expected[i]}", path + sep + str(i))
                    )
                elif i >= len(expected):
                    differences.append(
                        (
                            f"Result contains extra element {result[i]}",
                            path + sep + str(i),
                        )
                    )
                else:
                    differences += get_differences(
                        expected[i], result[i], path + sep + str(i)
                    )
        elif expected != result:
            differences.append((f"Expected={expected}, Result={result}", path))
    else:
        exp_keys, res_keys = set(expected.keys()), set(result.keys())
        in_exp_not_res, in_res_not_exp = exp_keys - res_keys, res_keys - exp_keys

        for k in in_exp_not_res:
            differences.append(("In Expected, not in Result", path + sep + k))
        for k in in_res_not_exp:
            differences.append(("In Result, not in Expected", path + sep + k))

        for k in exp_keys & res_keys:
            differences += get_differences(expected[k], result[k], path + sep + k)

    return differences


def print_and_test(expected, result=None, actual=None):
    """Print objects and differences, then test equality."""
    pp = pprint.PrettyPrinter(indent=2)
    if actual is not None:
        result = actual

    print("\nExpected:")
    pp.pprint(expected)

    print("\nActual:")
    pp.pprint(result)

    print("\nDifferences:")
    print("\n".join([" - ".join(v) for v in get_differences(expected, result)]))

    assert result == expected


def get_mock_bq_client(schema: List[SchemaField]):
    """Get a mock BQ client that will return a specified schema."""

    class MockClient:
        """Mock bigquery.Client."""

        def get_table(self, table_ref):
            """Mock bigquery.Client.get_table."""
            return bigquery.Table(table_ref, schema=schema)

    return MockClient()
