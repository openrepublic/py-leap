#!/usr/bin/env python3

from typing import (
    Iterator, Optional, Union, Tuple, List, Dict
)


ExecutionResult = Tuple[int, str]
ExecutionStream = Tuple[str, Iterator[str]]

TransactionResult = Dict[str, Union[str, int]]

ActionResult = Tuple[int, Union[TransactionResult, str]]
