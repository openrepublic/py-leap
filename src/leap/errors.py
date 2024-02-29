#!/usr/bin/env python3


class SerializationException(BaseException):
    ...


class ContractDeployError(BaseException):
    ...


class TransactionPushError(BaseException):

    '''
    example error:
    {
        "code": 3050003,
        "name": "eosio_assert_message_exception",
        "what": "eosio_assert_message assertion failure",
        "details": [
            {
                "message": "assertion failure with message: action cancelled",
                "file": "cf_system.cpp",
                "line_number": 14,
                "method": "eosio_assert"
            },
            {
                "message": "pending console output: VAR1: hello world!",
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
        details: list[dict]
    ):
        self.code = code
        self.name = name
        self.what = what

        msg = f'Error code {code}: {what}'

        self.pending_output = ''
        if len(details) > 0:
            for detail in details:
                msg = detail['message']
                if 'pending console output: ' in msg:
                    self.pending_output = msg.replace('pending console output: ', '')

                else:
                    msg += f' {detail["message"]}'

        super().__init__(msg)

    @staticmethod
    def from_json(error: dict):
        return TransactionPushError(
            error['code'],
            error['name'],
            error['what'],
            error['details']
        )


class ChainAPIError(BaseException):
    ...
