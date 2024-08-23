import msgspec


class ABIStructField(msgspec.Struct):
    type: str
    name: str


class ABIStruct(msgspec.Struct):
    name: str
    fields: list[ABIStructField]
    base: str | None = None


class ABIType(msgspec.Struct):
    new_type_name: str
    type: str


class ABIVariant(msgspec.Struct):
    name: str
    types: list[str]


class ABITable(msgspec.Struct):
    name: str
    type: str
    key_names: list[str]


class ABI(msgspec.Struct):
    version: str
    structs: list[ABIStruct]
    types: list[ABIType]
    variants: list[ABIVariant]
    tables: list[ABITable]
