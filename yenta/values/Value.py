from dataclasses import dataclass
from typing import Any, List, Dict, Union, Set, Tuple
from enum import Enum


class ValueType(str, Enum):

    SCALAR = 'scalar'
    LIST = 'list'
    TUPLE = 'tuple'
    DICT = 'dict'
    SET = 'set'


@dataclass
class Value:

    value: Any
    value_type: ValueType = ValueType.SCALAR

    def __post_init__(self):

        if self.value_type == ValueType.LIST:
            self.value = list(self.value)
        elif self.value_type == ValueType.TUPLE:
            self.value = tuple(self.value)
        elif self.value_type == ValueType.DICT:
            self.value = dict(self.value)
        elif self.value_type == ValueType.SET:
            self.value = set(self.value)
