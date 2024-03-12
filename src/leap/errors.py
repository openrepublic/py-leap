#!/usr/bin/env python3

from typing import Any


class SerializationException(Exception):
    ...


class ChainAPIError(Exception):
    '''
    example error:
    {
        "code": 3050003,
        "name": "eosio_assert_message_exception",
        "what": "eosio_assert_message assertion failure",
        "details": [
            {
                "message": "assertion failure with message: {eosio::check msg}",
                "file": "cf_system.cpp",
                "line_number": 14,
                "method": "eosio_assert"
            },
            {
                "message": "pending console output: hello world!",
                "file": "apply_context.cpp",
                "line_number": 124,
                "method": "exec_one"
            }
        ]
    }
    '''

    def __init__(
        self,
        code: int,
        name: str,
        what: str,
        details: list[dict[str, str]]
    ):
        self.code = code
        self.name = name
        self.what = what
        self.details = details

        msg = f'{code}: {what}'

        self.messages: list[str] = []

        self.pending_output: str = ''
        for detail in self.details:
            msg = detail['message']
            if 'pending console output: ' in msg:
                self.pending_output = msg.replace('pending console output: ', '')

            else:
                detail_msg = detail['message']
                self.messages.append(detail_msg)
                msg += f' {detail_msg}'

        super().__init__(msg)

    @staticmethod
    def is_json_error(obj: Any) -> bool:
        return all((
            isinstance(obj, dict),
            'code' in obj and isinstance(obj['code'], int),
            'name' in obj and isinstance(obj['name'], str),
            'what' in obj and isinstance(obj['what'], str),
            'details' in obj and isinstance(obj['details'], list)
        ))

    @classmethod
    def from_other(cls, other: 'ChainAPIError') -> 'ChainAPIError':
        return cls(
            other.code,
            other.name,
            other.what,
            other.details
        )

    @classmethod
    def from_json(cls, err: dict):
        return cls(
            err['code'],
            err['name'],
            err['what'],
            err['details']
        )

    def __repr__(self) -> str:
        rep = ', '.join((
            f'ChainAPIError [{self.code}]: {self.what}',
            *[f'detail msg {i + 1}: {m}' for i, m in enumerate(self.messages)],
        ))

        if self.pending_output:
            rep += f', {self.pending_output}'

        return rep


class ChainHTTPError(Exception):
    '''
    example error:
    {
        "code": 500,
        "message": "Internal Service Error",
        "error": {chain api error}
    }
    '''

    def __init__(
        self,
        code: int,
        message: str,
        error: ChainAPIError
    ):
        super().__init__(message)
        self.code = code
        self.error = error

    @staticmethod
    def is_json_error(obj: Any) -> bool:
        return all((
            isinstance(obj, dict),
            'code' in obj and isinstance(obj['code'], int) and (obj['code'] >= 400 and obj['code'] <= 599),
            'message' in obj and isinstance(obj['message'], str),
            'error' in obj and ChainAPIError.is_json_error(obj['error'])
        ))

    @classmethod
    def from_json(cls, err: dict):
        return cls(
            err['code'],
            err['message'],
            ChainAPIError.from_json(err['error'])
        )


class TransactionPushError(ChainAPIError):
    ...


class ContractDeployError(TransactionPushError):
    ...
