"""Utility functions for tests."""

import pprint


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


class MockDryRunContext:
    """Mock DryRunContext."""

    def __init__(
        self,
        cls,
        use_cloud_function=False,
        id_token=None,
        credentials=None,
    ):
        """Initialize dry run instance."""
        self.use_cloud_function = use_cloud_function
        self.id_token = id_token
        self.credentials = credentials
        self.cls = cls

    def create(
        self,
        sql=None,
        project="moz-fx-data-shared-prod",
        dataset=None,
        table=None,
    ):
        """Initialize passed MockDryRun instance."""
        return self.cls(
            use_cloud_function=self.use_cloud_function,
            id_token=self.id_token,
            credentials=self.credentials,
            sql=sql,
            project=project,
            dataset=dataset,
            table=table,
        )


class MockDryRun:
    """Mock dryrun.DryRun."""

    def __init__(
        self,
        use_cloud_function,
        id_token,
        credentials,
        sql=None,
        project=None,
        dataset=None,
        table=None,
    ):
        """Create MockDryRun instance."""
        self.sql = sql
        self.project = project
        self.dataset = dataset
        self.table = table
        self.use_cloud_function = use_cloud_function
        self.credentials = credentials
        self.id_token = id_token
