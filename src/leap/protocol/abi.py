import msgspec


class ABIType(msgspec.Struct):
    new_type_name: str
    type: str


class ABIStructField(msgspec.Struct):
    type: str
    name: str


class ABIStruct(msgspec.Struct):
    name: str
    fields: list[ABIStructField]
    base: str | None = None


class ABIAction(msgspec.Struct):
    name: str
    type: str
    ricardian_contract: str = ''


class ABITable(msgspec.Struct):
    name: str
    type: str
    key_names: list[str]


class ABIClause(msgspec.Struct):
    id: str
    body: str


class ABIVariant(msgspec.Struct):
    name: str
    types: list[str]


class ABI(msgspec.Struct):
    version: str
    types: list[ABIType] = []
    structs: list[ABIStruct] = []
    actions: list[ABIAction] = []
    tables: list[ABITable] = []
    ricardian_clauses: list[ABIClause] = []
    error_messages: list[str] = []
    abi_extensions: list[str] = []
    variants: list[ABIVariant] = []
