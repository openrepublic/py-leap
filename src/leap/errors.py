#!/usr/bin/env python3


class SerializationException(BaseException):
    ...


class ContractDeployError(BaseException):
    ...


class TransactionPushError(BaseException):

    def __init__(self, message: str):
        self.message = message
        super().__init__(message)


class ChainAPIError(BaseException):
    ...
