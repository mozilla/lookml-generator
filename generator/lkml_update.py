"""An updated lkml parser to handle explore queries."""
from typing import List, Union

from lkml.keys import KEYS_WITH_NAME_FIELDS
from lkml.simple import DictParser
from lkml.tree import BlockNode, DocumentNode, ListNode, PairNode


def dump(obj: dict) -> str:
    """Dump an object as LookML."""
    parser = UpdatedDictParser()
    tree: DocumentNode = parser.parse(obj)
    return str(tree)


class UpdatedDictParser(DictParser):
    """An updated DictParser that properly handles queries."""

    def parse_any(
        self, key: str, value: Union[str, list, tuple, dict]
    ) -> Union[
        List[Union[BlockNode, ListNode, PairNode]], BlockNode, ListNode, PairNode
    ]:
        """Dynamically serializes a Python object based on its type.

        Args:
            key: A LookML field type (e.g. "suggestions" or "hidden")
            value: A string, tuple, or list to serialize
        Raises:
            TypeError: If input value is not of a valid type
        Returns:
            A generator of serialized string chunks
        """
        if isinstance(value, str):
            return self.parse_pair(key, value)
        elif isinstance(value, (list, tuple)):
            if self.is_plural_key(key) and not self.parent_key == "query":
                # See https://github.com/joshtemple/lkml/issues/53
                # We check that the parent is not a query to ensure the
                # query fields don't get unnested
                return self.expand_list(key, value)
            else:
                return self.parse_list(key, value)
        elif isinstance(value, dict):
            if key in KEYS_WITH_NAME_FIELDS or "name" not in value.keys():
                name = None
            else:
                name = value.pop("name")
            return self.parse_block(key, value, name)
        else:
            print(f"Accessing key {key!r}")
            raise TypeError("Value must be a string, list, tuple, or dict.")
