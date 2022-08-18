import dataclasses
import inspect
import json
import re
import types
import typing
from os import GenericAlias
from re import Pattern
from abc import abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Any, Optional, TypeVar, Type, Generic, Callable

_issue_url = "https://github.com/arcalot/arcaflow-plugin-sdk-python/issues"


# region Exceptions

@dataclass
class ConstraintException(Exception):
    """
    ConstraintException indicates that the passed data violated one or more constraints defined in the 
    """
    path: typing.Tuple[str] = tuple([])
    msg: str = ""

    def __str__(self):
        if len(self.path) == 0:
            return "Validation failed: {}".format(self.msg)
        return "Validation failed for '{}': {}".format(" -> ".join(self.path), self.msg)


@dataclass
class NoSuchStepException(Exception):
    """
    NoSuchStepException indicates that the given step is not supported by a 
    """
    step: str

    def __str__(self):
        return "No such step: %s" % self.step


@dataclass
class BadArgumentException(Exception):
    """
    BadArgumentException indicates that an invalid configuration was passed to a schema component.
    """
    msg: str

    def __str__(self):
        return self.msg


@dataclass
class InvalidAnnotationException(Exception):
    annotation: str
    msg: str

    def __str__(self):
        return "Invalid {} annotation: {}".format(self.annotation, self.msg)


class SchemaBuildException(Exception):
    def __init__(self, path: typing.Tuple[str], msg: str):
        self.path = path
        self.msg = msg

    def __str__(self) -> str:
        if len(self.path) == 0:
            return "Invalid schema definition: %s" % self.msg
        return "Invalid schema definition for %s: %s" % (" -> ".join(self.path), self.msg)


class InvalidInputException(Exception):
    """
    This exception indicates that the input data for a given step didn't match the
    """
    constraint: ConstraintException

    def __init__(self, cause: ConstraintException):
        self.constraint = cause

    def __str__(self):
        return self.constraint.__str__()


class InvalidOutputException(Exception):
    """
    This exception indicates that the output of a schema was invalid. This is always a bug in the plugin and should
    be reported to the plugin author.
    """
    constraint: ConstraintException

    def __init__(self, cause: ConstraintException):
        self.constraint = cause

    def __str__(self):
        return self.constraint.__str__()


# endregion

# region Annotations

def name(name: str) -> typing.Callable[
    [typing.ForwardRef["PropertySchema"]], typing.ForwardRef["PropertySchema"]
]:
    """
    This annotation applies a name to a given field in an object.
    :param name: The name to apply
    :return: Callable
    """

    def call(t: typing.ForwardRef("PropertySchema")) -> typing.ForwardRef["PropertySchema"]:
        if not isinstance(t, PropertySchema):
            raise InvalidAnnotationException("name", "expected a property, found {}".format(type(t).__name__))
        if t.display is None:
            t.display = DisplayValue(name)
        else:
            t.display = name
        return t

    return call


_name = name


def description(description: str) -> typing.Callable[
    [typing.ForwardRef["PropertySchema"]], typing.ForwardRef["PropertySchema"]
]:
    def call(t: typing.ForwardRef("PropertySchema")) -> typing.ForwardRef["PropertySchema"]:
        if not isinstance(t, PropertySchema):
            raise InvalidAnnotationException("description", "expected a property, found {}".format(type(t).__name__))
        if t.display is None:
            t.display = DisplayValue()
        else:
            t.display = description
        return t

    return call


_description = description


def units(units: typing.ForwardRef("Units")):
    def call(
            t: typing.Union[typing.ForwardRef("IntSchema"), typing.ForwardRef("FloatSchema")]
    ) -> typing.ForwardRef["PropertySchema"]:
        if not isinstance(t, IntSchema) and not isinstance(t, FloatSchema):
            raise InvalidAnnotationException("units", "expected int or float schema, found {}".format(type(t).__name__))
        t.units = units
        return t

    return call


_units = units


def example(example: typing.Any) -> typing.Callable[
    [typing.ForwardRef["PropertySchema"]], typing.ForwardRef["PropertySchema"]
]:
    """
    This annotation provides the option to add an example to a type.
    :param example: the example as raw type, serializable by json.dumps. Do not use dataclasses
    """
    try:
        marshalled_example = json.dumps(example)
    except Exception as e:
        raise InvalidAnnotationException("example", "expected a JSON-serializable type, {}".format(e.__str__())) from e

    def call(t: typing.ForwardRef("PropertySchema")) -> typing.ForwardRef["PropertySchema"]:
        if not isinstance(t, PropertySchema):
            raise InvalidAnnotationException("example", "expected a property, found {}".format(type(t).__name__))
        if t.examples is None:
            t.examples = []
        t.examples.append(marshalled_example)
        return t

    return call


_example = example

DiscriminatorT = typing.TypeVar("DiscriminatorT", bound=typing.ForwardRef("OneOfSchema"))
Discriminator = typing.Callable[[typing.ForwardRef("OneOfSchema")], typing.ForwardRef("OneOfSchema")]


def discriminator(discriminator_field_name: str) -> Discriminator:
    """
    This annotation is used to manually set the discriminator field on a Union type.

    For example:

    typing.Annotated[typing.Union[A, B], annotations.discriminator("my_discriminator")]

    :param discriminator_field_name: the name of the discriminator field.
    :return: the callable decorator
    """

    def call(t: typing.ForwardRef("OneOfSchema")) -> typing.ForwardRef("OneOfSchema"):
        if not isinstance(t, OneOfStringSchema) and not isinstance(t, OneOfIntSchema):
            raise InvalidAnnotationException(
                "discriminator",
                "expected a property or object type with union member, found {}".format(type(t).__name__, )
            )
        oneof: typing.Union[OneOfStringSchema, OneOfIntSchema] = t

        one_of: typing.Dict[DiscriminatorT, ObjectSchema] = {}
        if isinstance(t, OneOfStringSchema):
            discriminator_field_schema = StringSchema()
        else:
            discriminator_field_schema = IntSchema()
        for key, item in oneof.types.items():
            if discriminator_field_name in item.properties:
                if discriminator_field_schema is not None and item.properties[discriminator_field_name].type:
                    raise InvalidAnnotationException(
                        "discriminator",
                        "Discriminator field mismatch, the discriminator field must have the same type across all "
                        "dataclasses in a Union type."
                    )
                discriminator_field_schema = item.properties[discriminator_field_name].type
            if hasattr(item, "__discriminator_value"):
                one_of[item.__discriminator_value] = item
            else:
                one_of[key] = item

        oneof.discriminator_field_name = discriminator_field_name

        for key, item in oneof.types.items():
            try:
                discriminator_field_schema.validate(key)
            except ConstraintException as e:
                raise BadArgumentException(
                    "The discriminator value has an invalid value: {}. "
                    "Please check your annotations.".format(
                        e.__str__()
                    )
                ) from e

        return oneof

    return call


_discriminator = discriminator


def discriminator_value(discriminator_value: typing.Union[str, int]):
    """
    This annotation adds a custom value for an instance of a discriminator. The value must match the discriminator field
     This annotation works only when used in conjunction with discriminator().

    For example:

    typing.Annotated[typing.Union[A
        typing.Annotated[A, annotations.discriminator_value("foo"),
        typing.Annotated[B, annotations.discriminator_value("bar")
    ], annotations.discriminator("my_discriminator")]

    :param discriminator_value: The value for the discriminator field.
    :return: The callable decorator
    """

    def call(t):
        if not isinstance(t, ObjectSchema):
            raise InvalidAnnotationException(
                "discriminator_value",
                "discriminator_value is only valid for object types, not {}".format(type(t).__name__)
            )
        t.__discriminator_value = discriminator_value
        return t

    return call


_discriminator_value = discriminator_value

ValidatorT = TypeVar("ValidatorT", bound=typing.Union[
    typing.ForwardRef("IntSchema"),
    typing.ForwardRef("FloatSchema"),
    typing.ForwardRef("StringSchema"),
    typing.ForwardRef("ListSchema"),
    typing.ForwardRef("MapSchema"),
    typing.ForwardRef("PropertySchema"),
])

Validator = Callable[[ValidatorT], ValidatorT]


def min(param: typing.Union[int, float]) -> Validator:
    """
    This decorator creates a minimum length (strings), minimum number (int, float), or minimum element count (lists and
    maps) validation.
    :param param: The minimum number
    :return: the validator
    """

    def call(t: typing.Union[
        typing.ForwardRef("IntSchema"),
        typing.ForwardRef("FloatSchema"),
        typing.ForwardRef("StringSchema"),
        typing.ForwardRef("ListSchema"),
        typing.ForwardRef("MapSchema"),
        typing.ForwardRef("PropertySchema"),
    ]) -> typing.Union[
        typing.ForwardRef("IntSchema"),
        typing.ForwardRef("FloatSchema"),
        typing.ForwardRef("StringSchema"),
        typing.ForwardRef("ListSchema"),
        typing.ForwardRef("MapSchema"),
        typing.ForwardRef("PropertySchema"),
    ]:
        effective_t = t
        if isinstance(t, PropertySchema):
            effective_t = t.type
        if hasattr(effective_t, "min"):
            effective_t.min = param
        else:
            raise BadArgumentException(
                "min is valid only for STRING, INT, FLOAT, LIST, and MAP types, not for {} types.".format(t.__name__)
            )
        if isinstance(t, PropertySchema):
            t.type = effective_t
        return t

    return call


_min = min


def max(param: int) -> Validator:
    """
    This decorator creates a maximum length (strings), maximum number (int, float), or maximum element count (lists and
    maps) validation.
    :param param: The maximum number
    :return: the validator
    """

    def call(t: typing.Union[
        typing.ForwardRef("IntSchema"),
        typing.ForwardRef("FloatSchema"),
        typing.ForwardRef("StringSchema"),
        typing.ForwardRef("ListSchema"),
        typing.ForwardRef("MapSchema"),
        typing.ForwardRef("PropertySchema"),
    ]) -> typing.Union[
        typing.ForwardRef("IntSchema"),
        typing.ForwardRef("FloatSchema"),
        typing.ForwardRef("StringSchema"),
        typing.ForwardRef("ListSchema"),
        typing.ForwardRef("MapSchema"),
        typing.ForwardRef("PropertySchema"),
    ]:
        effective_t = t
        if isinstance(t, PropertySchema):
            effective_t = t.type
        if hasattr(effective_t, "max"):
            effective_t.max = param
        else:
            raise BadArgumentException(
                "max is valid only for STRING, INT, FLOAT, LIST, and MAP types, not for {} types.".format(t.__name__)
            )
        if isinstance(t, PropertySchema):
            t.type = effective_t
        return t

    return call


_max = max


def pattern(pattern: Pattern) -> Validator:
    """
    This decorator creates a regular expression pattern validation for strings.
    :param pattern: The regular expression.
    :return: the validator
    """

    def call(t: typing.Union[
        typing.ForwardRef("StringSchema"),
        typing.ForwardRef("PropertySchema"),
    ]) -> typing.Union[
        typing.ForwardRef("StringSchema"),
        typing.ForwardRef("PropertySchema"),
    ]:
        effective_t = t
        if isinstance(t, PropertySchema):
            effective_t = t.type
        if hasattr(effective_t, "pattern"):
            effective_t.pattern = pattern
        else:
            raise BadArgumentException("pattern is valid only for STRING types, not for {} types.".format(t.__name__))
        if isinstance(t, PropertySchema):
            t.type = effective_t
        return t

    return call


_pattern = pattern


def required_if(required_if: str) -> Validator:
    """
    This decorator creates a that marks the current field as required if the specified field is set.
    :param required_if: The other field to use.
    :return: the validator
    """

    def call(t: typing.ForwardRef("PropertySchema")) -> typing.ForwardRef("PropertySchema"):
        if not isinstance(t, PropertySchema):
            raise BadArgumentException("required_if is only valid for properties on object types.")
        require_if_list = list(t.required_if)
        require_if_list.append(required_if)
        t.required_if = require_if_list
        return t

    return call


_required_if = required_if


def required_if_not(required_if_not: str) -> Validator:
    """
    This decorator creates a validation that marks the current field as required if the specified field is not set. If
    there are multiple of these validators, the current field is only marked as required if none of the specified fields
    are provided.
    :param required_if_not: The other field to use.
    :return: the validator
    """

    def call(t: typing.ForwardRef("PropertySchema")) -> typing.ForwardRef("PropertySchema"):
        if not isinstance(t, PropertySchema):
            raise BadArgumentException("required_if_not is only valid for fields on object types.")
        required_if_not_list = list(t.required_if_not)
        required_if_not_list.append(required_if_not)
        t.required_if_not = required_if_not_list
        return t

    return call


_required_if_not = required_if_not


def conflicts(conflicts: str) -> Validator:
    """
    This decorator creates a validation that triggers if the current field on an object is set in parallel with the
    specified field.
    :param conflicts: The field to conflict with.
    :return: the validator
    """

    def call(t: typing.ForwardRef("PropertySchema")) -> typing.ForwardRef("PropertySchema"):
        if not isinstance(t, PropertySchema):
            raise BadArgumentException("conflicts is only valid for fields on object types.")
        conflicts_list = list(t.conflicts)
        conflicts_list.append(conflicts)
        t.conflicts = conflicts_list
        return t

    return call


_conflicts = conflicts

# endregion

# region Type aliases

VALUE_TYPE = typing.Annotated[
    typing.Union[
        typing.Annotated[typing.ForwardRef("StringEnumSchema"), discriminator_value("enum_string")],
        typing.Annotated[typing.ForwardRef("IntEnumSchema"), discriminator_value("enum_integer")],
        typing.Annotated[typing.ForwardRef("StringSchema"), discriminator_value("string")],
        typing.Annotated[typing.ForwardRef("PatternSchema"), discriminator_value("pattern")],
        typing.Annotated[typing.ForwardRef("IntSchema"), discriminator_value("integer")],
        typing.Annotated[typing.ForwardRef("FloatSchema"), discriminator_value("float")],
        typing.Annotated[typing.ForwardRef("BoolSchema"), discriminator_value("bool")],
        typing.Annotated[typing.ForwardRef("ListSchema"), discriminator_value("list")],
        typing.Annotated[typing.ForwardRef("MapSchema"), discriminator_value("map")],
        typing.Annotated[typing.ForwardRef("ObjectSchema"), discriminator_value("object")],
        typing.Annotated[
            typing.Union[
                typing.Annotated[typing.ForwardRef("OneOfStringSchema"), discriminator_value("string")],
                typing.Annotated[typing.ForwardRef("OneOfIntSchema"), discriminator_value("integer")]
            ],
            discriminator("discriminator_field_type"),
            discriminator_value("one_of"),
        ],
        typing.Annotated[typing.ForwardRef("RefSchema"), discriminator_value("ref")],
    ],
    discriminator("type_id")
]
MAP_KEY_TYPE = typing.Annotated[
    typing.Union[
        typing.Annotated[typing.ForwardRef("StringEnumSchema"), discriminator_value("enum_string")],
        typing.Annotated[typing.ForwardRef("IntEnumSchema"), discriminator_value("enum_integer")],
        typing.Annotated[typing.ForwardRef("StringSchema"), discriminator_value("string")],
        typing.Annotated[typing.ForwardRef("IntSchema"), discriminator_value("integer")],
    ],
    discriminator("type_id")
]
ID_TYPE = typing.Annotated[
    str,
    min(1),
    max(255),
    pattern(re.compile("^[$@a-zA-Z0-9-_]+$"))
]


# endregion

# region Schema

@dataclass
class Unit:
    """
    A unit is a description of a single scale of measurement, such as a "second". If there are multiple scales, such as
    "minute", "second", etc. then multiple of these unit classes can be composed into units.
    """
    name_short_singular: typing.Annotated[
        str,
        name("Short name (singular)"),
        description("Short name that can be printed in a few characters, singular form."),
        example("B"),
        example("char"),
    ]
    name_short_plural: typing.Annotated[
        str,
        name("Short name (plural)"),
        description("Short name that can be printed in a few characters, plural form."),
        example("B"),
        example("chars"),
    ]
    name_long_singular: typing.Annotated[
        str,
        name("Long name (singular)"),
        description("Longer name for this unit in singular form."),
        example("byte"),
        example("character"),
    ]
    name_long_plural: typing.Annotated[
        str,
        name("Long name (plural)"),
        description("Longer name for this unit in plural form."),
        example("bytes"),
        example("characters"),
    ]


@dataclass
class Units:
    """
    Units holds several scales of magnitude of the same unit, for example 5m30s.
    """
    base_unit: typing.Annotated[
        Unit,
        name("Base unit"),
        description("The base unit is the smallest unit of scale for this set of units.")
    ]
    multipliers: typing.Annotated[
        Optional[Dict[int, Unit]],
        name("Multipliers"),
        description("A set of multiplies that describe multiple units of scale."),
        example(
            {
                1024: Unit("kB", "kB", "kilobyte", "kilobytes"),
                1048576: Unit("MB", "MB", "megabyte", "megabytes")
            }
        )
    ] = None


UNIT_BYTE = Units(
    Unit(
        "B",
        "B",
        "byte",
        "bytes"
    ),
    {
        1024: Unit(
            "kB",
            "kB",
            "kilobyte",
            "kilobytes"
        ),
        1048576: Unit(
            "MB",
            "MB",
            "megabyte",
            "megabytes"
        ),
        1073741824: Unit(
            "GB",
            "GB",
            "gigabyte",
            "gigabytes"
        ),
        1099511627776: Unit(
            "TB",
            "TB",
            "terabyte",
            "terabytes"
        ),
        1125899906842624: Unit(
            "PB",
            "PB",
            "petabyte",
            "petabytes"
        ),
    }
)
UNIT_TIME = Units(
    Unit(
        "ns",
        "ns",
        "nanosecond",
        "nanoseconds"
    ),
    {
        1000: Unit(
            "ms",
            "ms",
            "microsecond",
            "microseconds"
        ),
        1000000: Unit(
            "s",
            "s",
            "second",
            "seconds"
        ),
        60000000: Unit(
            "m",
            "m",
            "minute",
            "minutes"
        ),
        3600000000: Unit(
            "H",
            "H",
            "hour",
            "hours"
        ),
        86400000000: Unit(
            "d",
            "d",
            "day",
            "days"
        ),
    }
)
UNIT_CHARACTER = Units(
    Unit(
        "char",
        "chars",
        "character",
        "characters"
    )
)
UNIT_PERCENT = Units(
    Unit(
        "%",
        "%",
        "percent",
        "percent"
    )
)


@dataclass
class DisplayValue:
    """
    This class holds the fields related to displaying an item in a user interface.
    """
    name: typing.Annotated[
        typing.Optional[str],
        name("Name"),
        description("Short text serving as a name or title for this item."),
        example("Fruit"),
        min(1),
    ] = None
    description: typing.Annotated[
        Optional[str],
        _name("Description"),
        _description("Description for this item if needed."),
        _example("Please select the fruit you would like."),
        _min(1),
    ] = None
    icon: typing.Annotated[
        Optional[str],
        _name("Icon"),
        _description(
            "SVG icon for this item. Must have the declared size of 64x64, must not include "
            "additional namespaces, and must not reference external resources."
        ),
        _example("<svg ...></svg>"),
        _min(1),
    ] = None


@dataclass
class StringEnumSchema:
    """
    This class specifically holds an enum that has string values.
    """
    values: typing.Annotated[
        Dict[str, DisplayValue],
        _min(1),
        _name("Values"),
        _description(
            "Mapping where the left side of the map holds the possible value and the right side holds the display "
            "value for forms, etc. "
        ),
        _example({"apple": {"name": "Apple"}, "orange": {"name": "Orange"}})
    ]


@dataclass
class IntEnumSchema:
    """
    This class specifically holds an enum that has integer values.
    """
    values: typing.Annotated[
        Dict[int, DisplayValue],
        min(1),
        name("Values"),
        description("Possible values for this field."),
        example({1024: DisplayValue("kB"), 1048576: DisplayValue("MB")})
    ]
    units: Optional[Units] = None


@dataclass
class StringSchema:
    """
    This class holds schema information for strings.
    """
    min: typing.Annotated[
        Optional[int],
        _min(0),
        _name("Minimum length"),
        _description("Minimum length for this string (inclusive)."),
        _units(UNIT_CHARACTER),
        _example(5),
    ] = None
    max: typing.Annotated[
        Optional[int],
        _min(0),
        _name("Maximum length"),
        _description("Maximum length for this string (inclusive)."),
        _units(UNIT_CHARACTER),
        _example(16)
    ] = None
    pattern: typing.Annotated[
        Optional[re.Pattern],
        _name("Pattern"),
        _description("Regular expression this string must match."),
        _example(re.compile("^[a-zA-Z]+$"))
    ] = None


@dataclass
class PatternSchema:
    """
    This class holds the schema information for regular expression patterns.
    """


@dataclass
class IntSchema:
    """
    This class holds the schema information for 64-bit integers. This type must also be able to unserialize
    strings that either hold raw number, of it a unit is set, a unit specification, e.g 5m30s.
    """
    min: typing.Annotated[
        Optional[int],
        _name("Minimum value"),
        _description("Minimum value for this int (inclusive)."),
        _example(5),
    ] = None
    max: typing.Annotated[
        Optional[int],
        _name("Maximum value"),
        _description("Maximum value for this int (inclusive)."),
        _example(16)
    ] = None
    units: typing.Annotated[
        Optional[Units],
        _name("Units"),
        _description("Units this number represents."),
        _example(UNIT_CHARACTER),
    ] = None


@dataclass
class FloatSchema:
    """
    This class holds the schema information for 64-bit floating point numbers. This type must be able to unserialize
    from strings.
    """
    min: typing.Annotated[
        Optional[float],
        _name("Minimum value"),
        _description("Minimum value for this int (inclusive)."),
        _example(5.0),
    ] = None
    max: typing.Annotated[
        Optional[float],
        _name("Maximum value"),
        _description("Maximum value for this int (inclusive)."),
        _example(16.0)
    ] = None
    units: typing.Annotated[
        Optional[Units],
        _name("Units"),
        _description("Units this number represents."),
        _example(UNIT_PERCENT)
    ] = None


@dataclass
class BoolSchema:
    """
    This class holds the schema information for boolean types. This type will unserialize from booleans, integers, and
    strings according to the parameters set.
    """


@dataclass
class ListSchema:
    """
    This class holds the schema definition for lists.
    """
    items: typing.Annotated[
        VALUE_TYPE,
        _name("Items"),
        _description("Type definition for items in this list."),
    ]
    min: typing.Annotated[
        Optional[int],
        _min(0),
        _name("Minimum items"),
        _description("Minimum number of items in this list."),
        _example(5),
    ] = None
    max: typing.Annotated[
        Optional[int],
        _min(0),
        _name("Maximum items"),
        _description("Maximum number of items in this list."),
        _example(16)
    ] = None


@dataclass
class MapSchema:
    """
    This class holds the schema definition for key-value associations.
    """
    keys: typing.Annotated[
        MAP_KEY_TYPE,
        _name("Keys"),
        _description("Type definition for map keys."),
    ]
    values: typing.Annotated[
        VALUE_TYPE,
        _name("Values"),
        _description("Type definition for map values."),
    ]
    min: typing.Annotated[
        Optional[int],
        _min(0),
        _name("Minimum items"),
        _description("Minimum number of items in this list."),
        _example(5),
    ] = None
    max: typing.Annotated[
        Optional[int],
        _min(0),
        _name("Maximum items"),
        _description("Maximum number of items in this list."),
        _example(16)
    ] = None


@dataclass
class PropertySchema:
    """
    This class holds the schema definition for a single object property.
    """
    type: typing.Annotated[
        VALUE_TYPE,
        _name("Type"),
        _description("Type definition for this field.")
    ]
    display: typing.Annotated[
        Optional[DisplayValue],
        _name("Display options"),
        _description("Display options for this property.")
    ] = None
    default: typing.Annotated[
        Optional[str],
        _name("Default"),
        _description(
            "Default value for this property in JSON encoding. The value must be unserializable by the type specified "
            "in the type field. "
        )
    ] = None,
    examples: typing.Annotated[
        Optional[List[str]],
        _name("Examples"),
        _description(
            "Example values for this property, encoded as JSON."
        )
    ] = None,
    required: typing.Annotated[
        Optional[bool],
        _name("Required"),
        _description("When set to true, the value for this field must be provided under all circumstances."),
        _conflicts("required_if"),
        _conflicts("required_if_not"),
    ] = False
    required_if: typing.Annotated[
        Optional[List[str]],
        _name("Required if"),
        _description(
            "Sets the current property to required if any of the properties in this list are set."
        )
    ] = frozenset([])
    required_if_not: typing.Annotated[
        Optional[List[str]],
        _name("Required if not"),
        _description(
            "Sets the current property to be required if none of the properties in this list are set."
        )
    ] = frozenset([])
    conflicts: typing.Annotated[
        Optional[List[str]],
        _name("Conflicts"),
        _description(
            "The current property cannot be set if any of the listed properties are set."
        )
    ] = frozenset([])


@dataclass
class ObjectSchema:
    """
    This class holds the definition for objects comprised of defined fields.
    """
    properties: typing.Annotated[
        Dict[str, PropertySchema],
        _name("Properties"),
        _description("Properties of this object.")
    ]


@dataclass
class OneOfStringSchema:
    """
    This class holds the definition of variable types with a string discriminator. This type acts as a split for a case
    where multiple possible object types can be present in a field. This type requires that there be a common field
    (the discriminator) which tells a parsing party which type it is. The field type in this case is a string.
    """
    types: Dict[str, ObjectSchema]
    discriminator_field_name: typing.Annotated[
        str,
        _name("Discriminator field name"),
        _description(
            "Name of the field used to discriminate between possible values. If this field is"
            "present on any of the component objects it must also be a string."
        )
    ] = "_type"


@dataclass
class OneOfIntSchema:
    """
    This class holds the definition of variable types with an integer discriminator. This type acts as a split for a
    case where multiple possible object types can be present in a field. This type requires that there be a common field
    (the discriminator) which tells a parsing party which type it is. The field type in this case is an integer.
    """
    types: Dict[int, ObjectSchema]
    discriminator_field_name: typing.Annotated[
        str,
        _name("Discriminator field name"),
        _description(
            "Name of the field used to discriminate between possible values. If this field is"
            "present on any of the component objects it must also be an int."
        )
    ] = "_type"


@dataclass
class RefSchema:
    """
    This class holds the definition of a reference to a scope-wide object.
    """
    id: typing.Annotated[
        ID_TYPE,
        _name("ID"),
        _description("Referenced object ID.")
    ]


@dataclass
class ScopeSchema:
    """
    A scope holds a root object schema, and several other objects that can be referenced using a RefSchema.
    """
    root: typing.Annotated[
        str,
        _name("Root object"),
        _description("Reference to the root object of this scope")
    ]
    objects: typing.Annotated[
        Dict[
            ID_TYPE,
            ObjectSchema
        ],
        _name("Objects"),
        _description("A set of referencable objects. These objects may contain references themselves.")
    ]


@dataclass
class StepOutputSchema:
    """
    This class holds the possible outputs of a step and the metadata information related to these outputs.
    """
    schema: typing.Annotated[
        ScopeSchema,
        _name("Schema"),
        _description("Data schema for this particular output.")
    ]
    display: typing.Annotated[
        Optional[DisplayValue],
        _name("Display options"),
        _description("Display options for this output.")
    ] = None
    error: typing.Annotated[
        bool,
        _name("Error"),
        _description("If set to true, this output will be treated as an error output.")
    ] = False


@dataclass
class StepSchema:
    """
    This class holds the definition for a single step, it's input and output definitions.
    """
    input: typing.Annotated[
        ScopeSchema,
        _name("Input"),
        _description("Input data schema")
    ]
    outputs: typing.Annotated[
        Dict[
            ID_TYPE,
            StepOutputSchema
        ],
        _name("Outputs"),
        _description("Possible outputs from this step.")
    ]
    display: typing.Annotated[
        Optional[DisplayValue],
        _name("Display options"),
        _description("Display options for this step.")
    ] = None


@dataclass
class Schema:
    """
    This is a collection of steps supported by a plugin.
    """
    steps: typing.Annotated[
        Dict[
            ID_TYPE,
            StepSchema
        ],
        _name("Steps"),
        _description("Steps this schema supports.")
    ]


# endregion

# region Types


TypeT = TypeVar("TypeT")


class AbstractType(Generic[TypeT]):
    """
    This class is an abstract class describing the methods needed to implement a type.
    """

    @abstractmethod
    def unserialize(self, data: Any, path: typing.Tuple[str] = tuple([])) -> TypeT:
        """
        This function takes the underlying raw data and decodes it into the underlying advanced data type (e.g.
        dataclass) for usage.
        :param data: the raw data.
        :param path: the list of structural elements that lead to this point for error messages.
        :return: the advanced datatype.
        :raise ConstraintException: if the passed data was not valid.
        """
        pass

    @abstractmethod
    def validate(self, data: TypeT, path: typing.Tuple[str] = tuple([])):
        """
        This function validates an already unserialized data type and raises an exception if it does not match
        the type definition.
        :param data: the unserialized data.
        :param path: the path that lead to this validation call, in order to produce a nice error message
        :raise ConstraintException: if the passed data was not valid.
        """

    @abstractmethod
    def serialize(self, data: TypeT, path: typing.Tuple[str] = tuple([])) -> Any:
        """
        This function serializes the passed data into it's raw form for transport, e.g. string, int, dicts, list.
        :param data: the underlying data type to be serialized.
        :param path: the list of structural elements that lead to this point for error messages.
        :return: the raw datatype.
        :raise ConstraintException: if the passed data was not valid.
        """
        pass


EnumT = TypeVar("EnumT", bound=Enum)


class _EnumType(AbstractType, Generic[EnumT]):
    """
    StringEnumType is an implementation of StringEnumSchema.
    """

    _type: Type[EnumT]

    def unserialize(self, data: Any, path: typing.Tuple[str] = tuple([])) -> EnumT:
        if isinstance(data, Enum):
            if data not in self._type:
                raise ConstraintException(
                    path,
                    "'{}' is not a valid value for the enum '{}'".format(data, self._type.__name__)
                )
            return data
        else:
            for v in self._type:
                if v == data or v.value == data:
                    return v
            raise ConstraintException(path, "'{}' is not a valid value for '{}'".format(data, self._type.__name__))

    def validate(self, data: TypeT, path: typing.Tuple[str] = tuple([])):
        if isinstance(data, Enum):
            if data not in self._type:
                raise ConstraintException(
                    path,
                    "'{}' is not a valid value for the enum '{}'".format(data, self._type.__name__)
                )
        else:
            for v in self._type:
                if v == data or v.value == data:
                    return
            raise ConstraintException(path, "'{}' is not a valid value for '{}'".format(data, self._type.__name__))

    def serialize(self, data: EnumT, path: typing.Tuple[str] = tuple([])) -> Any:
        if data not in self._type:
            raise ConstraintException(
                path,
                "'{}' is not a valid value for the enum '{}'".format(data, self._type.__name__)
            )
        return data.value


class StringEnumType(_EnumType, StringEnumSchema):
    """
    This class represents an enum type that is a string.
    """

    def __init__(self, type: Type[EnumT]):
        self._type = type
        values: Dict[str, DisplayValue] = {}
        try:
            for value in self._type:
                if not isinstance(value.value, str):
                    raise BadArgumentException(
                        "{} on {} is not a string".format(value, type.__name__))
                values[value] = DisplayValue(
                    value,
                )
            self.values = values
        except TypeError as e:
            raise BadArgumentException("{} is not a valid enum, not iterable".format(type.__name__)) from e


class IntEnumType(_EnumType, IntEnumSchema):
    """
    This class represents an enum type that is an integer.
    """

    def __init__(self, type: Type[EnumT]):
        self._type = type
        values: Dict[int, DisplayValue] = {}
        try:
            for value in self._type:
                if not isinstance(value.value, int):
                    raise BadArgumentException(
                        "{} on {} is not a string".format(value, type.__name__))
                values[value] = DisplayValue(
                    value,
                )
            self.values = values
        except TypeError as e:
            raise BadArgumentException("{} is not a valid enum, not iterable".format(type.__name__)) from e


class BoolType(AbstractType, BoolSchema):
    def unserialize(self, data: Any, path: typing.Tuple[str] = tuple([])) -> TypeT:
        if isinstance(data, bool):
            return data
        if isinstance(data, int):
            if data == 0:
                return False
            if data == 1:
                return True
            raise ConstraintException(path, "Boolean value expected, integer found ({})".format(data))
        if isinstance(data, str):
            lower_str = data.lower()
            if lower_str == "yes" or \
                    lower_str == "on" or \
                    lower_str == "true" or \
                    lower_str == "enable" or \
                    lower_str == "enabled" or \
                    lower_str == "1":
                return True
            if lower_str == "no" or \
                    lower_str == "off" or \
                    lower_str == "false" or \
                    lower_str == "disable" or \
                    lower_str == "disabled" or \
                    lower_str == "0":
                return False
            raise ConstraintException(path, "Boolean value expected, string found ({})".format(data))

        raise ConstraintException(path, "Boolean value expected, {} found".format(type(data)))

    def validate(self, data: TypeT, path: typing.Tuple[str] = tuple([])):
        if not isinstance(data, bool):
            raise ConstraintException(path, "Boolean value expected, {} found".format(type(data)))

    def serialize(self, data: TypeT, path: typing.Tuple[str] = tuple([])) -> Any:
        if isinstance(data, bool):
            return data
        raise ConstraintException(path, "Boolean value expected, {} found".format(type(data)))


@dataclass
class StringType(AbstractType, StringSchema):
    """
    StringType represents a string of characters for human consumption.
    """

    def unserialize(self, data: Any, path: typing.Tuple[str] = tuple([])) -> str:
        if isinstance(data, int):
            data = str(data)
        self.validate(data, path)
        return data

    def validate(self, data: TypeT, path: typing.Tuple[str] = tuple([])):
        if not isinstance(data, str):
            raise ConstraintException(path, "Must be a string, {} given".format(type(data)))
        string: str = data
        if self.min is not None and len(string) < self.min:
            raise ConstraintException(
                path,
                "String must be at least {} characters, {} given".format(self.min, len(string))
            )
        if self.max is not None and len(string) > self.max:
            raise ConstraintException(
                path,
                "String must be at most {} characters, {} given".format(self.max, len(string))
            )
        if self.pattern is not None and not self.pattern.match(string):
            raise ConstraintException(
                path,
                "String must match the pattern {}".format(self.pattern.__str__())
            )

    def serialize(self, data: str, path: typing.Tuple[str] = tuple([])) -> any:
        self.validate(data, path)
        return data


class PatternType(AbstractType, PatternSchema):
    """
    PatternType represents a regular expression.
    """

    def unserialize(self, data: Any, path: typing.Tuple[str] = tuple([])) -> re.Pattern:
        if not isinstance(data, str):
            raise ConstraintException(path, "Must be a string")
        try:
            return re.compile(str(data))
        except TypeError as e:
            raise ConstraintException(path, "Invalid regular expression ({})".format(e.__str__()))
        except ValueError as e:
            raise ConstraintException(path, "Invalid regular expression ({})".format(e.__str__()))

    def validate(self, data: TypeT, path: typing.Tuple[str] = tuple([])):
        if not isinstance(data, re.Pattern):
            raise ConstraintException(path, "Not a regular expression")

    def serialize(self, data: re.Pattern, path: typing.Tuple[str] = tuple([])) -> Any:
        if not isinstance(data, re.Pattern):
            raise ConstraintException(path, "Must be a re.Pattern")
        return data.pattern


class IntType(AbstractType, IntSchema):
    """
    IntType represents an integer type, both positive or negative. It is designed to take a 64 bit value.
    """

    def unserialize(self, data: Any, path: typing.Tuple[str] = tuple([])) -> int:
        if isinstance(data, str):
            try:
                data = int(data)
            except ValueError as e:
                raise ConstraintException(path, "Must be an integer") from e

        self.validate(data, path)
        return data

    def validate(self, data: TypeT, path: typing.Tuple[str] = tuple([])):
        if not isinstance(data, int):
            raise ConstraintException(path, "Must be an integer, {} given".format(type(data).__name__))
        integer = int(data)
        if self.min is not None and integer < self.min:
            raise ConstraintException(path, "Must be at least {}".format(self.min))
        if self.max is not None and integer > self.max:
            raise ConstraintException(path, "Must be at most {}".format(self.max))

    def serialize(self, data: int, path: typing.Tuple[str] = tuple([])) -> Any:
        self.validate(data, path)
        return data


@dataclass
class FloatType(AbstractType):
    """
    IntType represents an integer type, both positive or negative. It is designed to take a 64 bit value.
    """

    _min: Optional[float] = None
    "Minimum value (inclusive) for this type."

    _max: Optional[float] = None
    "Maximum value (inclusive) for this type."

    def __init__(self, min: Optional[float] = None, max: Optional[float] = None):
        self._min = min
        self._max = max

        if min is not None and not isinstance(min, float) and not isinstance(min, float):
            raise BadArgumentException(
                "min on floats must be a float, {} given".format(type(min))
            )
        if max is not None and not isinstance(max, float):
            raise BadArgumentException(
                "max on floats must be a float, {} given".format(type(max))
            )
        if min is not None and max is not None and max < min:
            raise BadArgumentException(
                "The max parameter must be larger than or equal to the min parameter on FloatType, min: {} and max: {} "
                "given".format(min, max)
            )

    @property
    def min(self) -> typing.Optional[float]:
        return self._min

    @property
    def max(self) -> typing.Optional[float]:
        return self._max

    def unserialize(self, data: Any, path: typing.Tuple[str] = tuple([])) -> int:
        if isinstance(data, str):
            try:
                data = float(data)
            except ValueError as e:
                raise ConstraintException(path, "Must be an float") from e

        self.validate(data, path)
        return data

    def validate(self, data: TypeT, path: typing.Tuple[str] = tuple([])):
        if not isinstance(data, float):
            raise ConstraintException(path, "Must be a float, {} given".format(type(data).__name__))
        integer = float(data)
        if self.min is not None and integer < self.min:
            raise ConstraintException(path, "Must be at least {}".format(self.min))
        if self.max is not None and integer > self.max:
            raise ConstraintException(path, "Must be at most {}".format(self.max))

    def serialize(self, data: float, path: typing.Tuple[str] = tuple([])) -> Any:
        self.validate(data, path)
        return data


ListT = TypeVar("ListT", bound=List)


@dataclass
class ListType(AbstractType, ListSchema, Generic[ListT]):
    """
    ListType is a strongly typed list that can have elements of only one type.
    """

    def unserialize(self, data: Any, path: typing.Tuple[str] = tuple([])) -> ListT:
        if not isinstance(data, list):
            raise ConstraintException(path, "Must be a list, {} given".format(type(data).__name__))
        for i in range(len(data)):
            new_path = list(path)
            new_path.append(str(i))
            data[i] = self.items.unserialize(data[i], tuple(new_path))
        self._validate(data, path)
        return data

    def validate(self, data: TypeT, path: typing.Tuple[str] = tuple([])):
        self._validate(data, path)
        for i in range(len(data)):
            new_path = list(path)
            new_path.append(str(i))
            self.items.validate(data[i], tuple(new_path))

    def serialize(self, data: ListT, path: typing.Tuple[str] = tuple([])) -> Any:
        self._validate(data, path)
        result = []
        for i in range(len(data)):
            new_path = list(path)
            new_path.append(str(i))
            result.append(self.items.serialize(data[i], tuple(new_path)))
        return result

    def _validate(self, data, path):
        if not isinstance(data, list):
            raise ConstraintException(path, "Must be a list, {} given".format(type(data).__name__))
        if self.min is not None and len(data) < self.min:
            raise ConstraintException(path, "Must have at least {} items, {} given".format(self.min, len(data)))
        if self.max is not None and len(data) > self.max:
            raise ConstraintException(path, "Must have at most {} items, {} given".format(self.max, len(data)))


MapT = TypeVar("MapT", bound=Dict)


@dataclass
class MapType(AbstractType, MapSchema, Generic[MapT]):
    """
    MapType is a key-value dict with fixed types for both.
    """

    def _validate(self, data, path):
        if not isinstance(data, dict):
            raise ConstraintException(path, "Must be a dict, {} given".format(type(data).__name__))
        entries = dict(data)
        if self.min is not None and len(entries) < self.min:
            raise ConstraintException()
        if self.max is not None and len(entries) > self.max:
            raise ConstraintException()
        return entries

    def unserialize(self, data: Any, path: typing.Tuple[str] = tuple([])) -> MapT:
        entries = self._validate(data, path)
        result: MapT = {}
        for key in entries.keys():
            value = entries[key]
            new_path = list(path)
            new_path.append(key)
            key_path = list(tuple(new_path))
            key_path.append("key")
            unserialized_key = self.keys.unserialize(key, tuple(key_path))
            if unserialized_key in result:
                raise ConstraintException(
                    tuple(key_path),
                    "Key already exists in result dict"
                )
            value_path = list(tuple(new_path))
            value_path.append("value")
            result[unserialized_key] = self.values.unserialize(value, tuple(value_path))
        return result

    def validate(self, data: TypeT, path: typing.Tuple[str] = tuple([])):
        self._validate(data, path)
        for key in data.keys():
            value = data[key]
            new_path = list(path)
            new_path.append(key)
            key_path = list(tuple(new_path))
            key_path.append("key")
            self.keys.validate(key, tuple(key_path))
            value_path = list(tuple(new_path))
            value_path.append("value")
            self.values.validate(value, tuple(new_path))

    def serialize(self, data: MapT, path: typing.Tuple[str] = tuple([])) -> Any:
        entries = self._validate(data, path)
        result = {}
        for key in entries.keys():
            key_path = list(path)
            key_path.append(str(key))
            key_path.append("key")
            serialized_key = self.keys.serialize(key, tuple(key_path))
            value_path = list(path)
            value_path.append(str(key))
            value_path.append("value")
            value = self.values.serialize(data[key], tuple(value_path))
            result[serialized_key] = value
        entries = self._validate(result, path)
        return entries


FieldT = TypeVar("FieldT")


class PropertyType(PropertySchema, Generic[FieldT]):
    field_override: str = ""


ObjectT = TypeVar("ObjectT", bound=object)


@dataclass
class ObjectType(ObjectSchema, AbstractType, Generic[ObjectT]):
    """
    ObjectType represents an object with predefined fields. The property declaration must match the fields in the class.
    The type currently does not validate if the properties match the provided class.
    """

    _cls: Type[ObjectT]

    def __init__(
            self,
            cls: Type[ObjectT],
            properties: Dict[str, PropertyType]
    ):
        super().__init__(properties)
        self._cls = cls
        self._validate_config(cls, properties)

    @property
    def cls(self) -> Type[ObjectT]:
        return self._cls

    def _validate_config(self, cls: Type[ObjectT], properties: Dict[str, PropertyType]):
        if not isinstance(cls, type):
            raise BadArgumentException(
                "The passed class argument '{}' is not a type. Please pass a type.".format(type(cls).__name__)
            )
        if not isinstance(properties, dict):
            raise BadArgumentException(
                "The properties parameter to 'ObjectType' must be a 'dict', '{}' given".format(
                    type(properties).__name__
                )
            )
        try:
            dataclasses.fields(cls)
        except Exception as e:
            raise BadArgumentException(
                "The passed class '{}' is not a dataclass. Please use a dataclass.".format(cls.__name__)
            ) from e
        cls_dict = cls.__dict__
        params = inspect.signature(cls.__init__).parameters.items()
        if len(params) != len(properties) + 1:
            raise BadArgumentException(
                "The '{}' class has an invalid number of parameters in the '__init__' function. Expected: {} got: {}\n"
                "The '__init__' parameters must match your declared parameters exactly so the Arcaflow plugin SDK can "
                "inject the data values."
            )
        params_iter = iter(params)
        if len(properties) > 0:
            attribute_annotations = cls_dict["__annotations__"]
            next(params_iter)
            i = 0
            for property_id, property in properties.items():
                field_id = property_id
                if property.field_override != "":
                    field_id = property.field_override
                if field_id not in attribute_annotations:
                    raise BadArgumentException(
                        "The '{}' class does not contain a field called '{}' as required by the property '{}'.".format(
                            cls.__name__,
                            field_id,
                            property_id,
                        )
                    )
                param = next(params_iter)
                param_name = param[0]
                param_value: inspect.Parameter = param[1]
                if param_name != field_id:
                    raise BadArgumentException(
                        "Mismatching parameter name {} in the '__init__' function of '{}'. Expected: {} got: {} "
                        "Please make sure the parameters for your custom '__init__' function are in the same order as "
                        "you declared them in the dataclass.".format(i, cls.__name__, field_id, param_name)
                    )
                if param_value.annotation != attribute_annotations[field_id]:
                    raise BadArgumentException(
                        "Mismatching parameter type declarations for '{}' in the '__init__' function of '{}'. "
                        "Expected: {} got: {}. Please make sure that your '__init__' parameters have the same type "
                        "declarations as the properties declared on your dataclass.".format(
                            param_name,
                            cls.__name__,
                            attribute_annotations[field_id].__name__,
                            param_value.annotation.__name__
                        )
                    )
                i = i + 1

    def unserialize(self, data: Any, path: typing.Tuple[str] = tuple([])) -> ObjectT:
        if not isinstance(data, dict):
            raise ConstraintException(path, "Must be a dict, got {}".format(type(data).__name__))
        kwargs = {}
        for key in data.keys():
            if key not in self.properties:
                raise ConstraintException(
                    path,
                    "Invalid parameter '{}', expected one of: {}".format(key, ", ".join(self.properties.keys()))
                )
        for property_id in self.properties.keys():
            object_property: PropertyType = self.properties[property_id]
            property_value: Optional[any] = None
            try:
                property_value = data[property_id]
            except KeyError:
                pass
            new_path = list(path)
            new_path.append(property_id)
            if property_value is not None:
                field_id = property_id
                if object_property.field_override != "":
                    field_id = object_property.field_override
                kwargs[field_id] = object_property.type.unserialize(property_value, tuple(new_path))

                for conflict in object_property.conflicts:
                    if conflict in data:
                        raise ConstraintException(
                            tuple(new_path),
                            "Field conflicts '{}', set one of the two, not both".format(conflict)
                        )
            else:
                self._validate_not_set(data, object_property, tuple(new_path))
        return self._cls(**kwargs)

    def validate(self, data: TypeT, path: typing.Tuple[str] = tuple([])):
        if not isinstance(data, self._cls):
            raise ConstraintException(
                path,
                "Must be an instance of {}, {} given".format(self._cls.__name__, type(data).__name__)
            )
        values = {}
        for property_id in self.properties.keys():
            property_field: PropertyType = self.properties[property_id]
            field_id = property_id
            if property_field.field_override != "":
                field_id = property_field.field_override
            new_path, value = self._validate_property(data, path, field_id, property_id)
            if value is not None:
                property_field.type.validate(value, tuple(new_path))
                values[property_id] = value
        for property_id in self.properties.keys():
            property_field: PropertyType = self.properties[property_id]
            new_path = list(path)
            new_path.append(property_id)
            if property_id in values.keys():
                for conflicts in property_field.conflicts:
                    if conflicts in values.keys():
                        raise ConstraintException(
                            tuple(new_path),
                            "Field conflicts with {}".format(conflicts)
                        )
            else:
                if property_field.required:
                    raise ConstraintException(
                        tuple(new_path),
                        "Field is required but not set"
                    )
                if len(property_field.required_if_not) > 0:
                    found = False
                    for required_if_not in property_field.required_if_not:
                        if required_if_not in values.keys():
                            found = True
                            break
                    if not found:
                        raise ConstraintException(
                            tuple(new_path),
                            "Field is required because none of '{}' are set".format(
                                "', '".join(property_field.required_if_not))
                        )

                for required_if in property_field.required_if:
                    if required_if in values.keys():
                        raise ConstraintException(
                            tuple(new_path),
                            "Field is required because none of '{}' are set".format(
                                "', '".join(property_field.required_if_not))
                        )

    def serialize(self, data: ObjectT, path: typing.Tuple[str] = tuple([])) -> Any:
        if not isinstance(data, self._cls):
            raise ConstraintException(
                path,
                "Must be an instance of {}, {} given".format(self._cls.__name__, type(data).__name__)
            )
        result = {}
        for property_id in self.properties.keys():
            field_id = property_id
            property_field: PropertyType = self.properties[property_id]
            if property_field.field_override != "":
                field_id = property_field.field_override
            new_path, value = self._validate_property(data, path, field_id, property_id)
            if value is not None:
                result[property_id] = property_field.type.serialize(getattr(data, field_id), tuple(new_path))
        return result

    def _validate_property(self, data: TypeT, path: typing.Tuple[str], field_id: str, property_id: str):
        new_path = list(path)
        new_path.append(property_id)
        value = getattr(data, field_id)
        property_field: PropertyType = self.properties[property_id]
        if value is None:
            self._validate_not_set(data, property_field, tuple(new_path))
        return new_path, value

    @staticmethod
    def _validate_not_set(data, object_property: PropertyType, path: typing.Tuple[str]):
        if object_property.required:
            raise ConstraintException(
                path,
                "This field is required"
            )
        for required_if in object_property.required_if:
            if (isinstance(data, dict) and required_if in data) or \
                    (hasattr(data, required_if) and getattr(data, required_if) is None):
                raise ConstraintException(
                    path,
                    "This field is required because '{}' is set".format(required_if)
                )
        if len(object_property.required_if_not) > 0:
            none_set = True
            for required_if_not in object_property.required_if_not:
                if (isinstance(data, dict) and required_if_not in data) or \
                        (hasattr(data, required_if_not) and getattr(data, required_if_not) is not None):
                    none_set = False
                    break
            if none_set:
                if len(object_property.required_if_not) == 1:
                    raise ConstraintException(
                        path,
                        "This field is required because '{}' is not set".format(
                            object_property.required_if_not[0]
                        )
                    )
                raise ConstraintException(
                    path,
                    "This field is required because none of '{}' are set".format(
                        "', '".join(object_property.required_if_not)
                    )
                )


OneOfT = TypeVar("OneOfT", bound=object)
DiscriminatorT = TypeVar("DiscriminatorT", bound=typing.Union[str, int, Enum])


@dataclass
class OneOfStringType(OneOfStringSchema, AbstractType[OneOfT], Generic[OneOfT, DiscriminatorT]):
    """
    OneOfType is a type that can have multiple types of underlying objects. It only supports object types, and the
    differentiation is done based on a special discriminator field.

    Important rules:

    - One object type must appear only once.
    - If the discriminator field appears in the object type, it must have the same type as declared here, and must not
      be optional.
    - The discriminator field must be a string, int, or an enum.
    """

    def unserialize(self, data: Any, path: typing.Tuple[str] = tuple([])) -> OneOfT:
        if not isinstance(data, dict):
            raise ConstraintException(path, "Must be a dict, got {}".format(type(data).__name__))
        new_path = list(path)
        new_path.append(self.discriminator_field_name)
        if self.discriminator_field_name not in data:
            raise ConstraintException(tuple(new_path), "Required discriminator field not found")
        unserialized_discriminator_field: str = data[self.discriminator_field_name]
        if not isinstance(unserialized_discriminator_field, str):
            raise ConstraintException(
                tuple(new_path),
                "String required, {} found".format(type(unserialized_discriminator_field).__name__)
            )
        if unserialized_discriminator_field not in self.types:
            raise ConstraintException(
                tuple(new_path),
                "Invalid value for field: '{}' expected one of: '{}'".format(
                    unserialized_discriminator_field,
                    "', '".join(list(self.types.keys()))
                )
            )
        sub_type: ObjectType = self.types[unserialized_discriminator_field]
        if self.discriminator_field_name not in sub_type.properties:
            del data[self.discriminator_field_name]
        return sub_type.unserialize(data, path)

    def validate(self, data: OneOfT, path: typing.Tuple[str] = tuple([])):
        types = []
        for discriminator, item_schema in self.types.items():
            item_schema: ObjectType
            types.append(item_schema.cls.__name__)
            if isinstance(data, item_schema.cls):
                item_schema.validate(data)
                if self.discriminator_field_name in item_schema.properties:
                    if getattr(data, self.discriminator_field_name) != discriminator:
                        new_path = list(path)
                        new_path.append(self.discriminator_field_name)
                        raise ConstraintException(
                            tuple(new_path),
                            "Invalid value for '{}' on '{}', should be: '{}'".format(
                                self.discriminator_field_name,
                                item_schema.cls.__name__,
                                discriminator
                            )
                        )
                return
        raise ConstraintException(
            tuple(path),
            "Invalid type: '{}', expected one of '{}'".format(
                type(data).__name__,
                "', '".join(types)
            )
        )

    def serialize(self, data: OneOfT, path: typing.Tuple[str] = tuple([])) -> Any:
        types = []
        for discriminator, item_schema in self.types.items():
            item_schema: ObjectType
            types.append(item_schema.cls.__name__)
            if isinstance(data, item_schema.cls):
                serialized_data = item_schema.serialize(data)
                if self.discriminator_field_name in item_schema.properties:
                    if getattr(data, self.discriminator_field_name) != discriminator:
                        new_path = list(path)
                        new_path.append(self.discriminator_field_name)
                        raise ConstraintException(
                            tuple(new_path),
                            "Invalid value for '{}' on '{}', should be: '{}'".format(
                                self.discriminator_field_name,
                                item_schema.cls.__name__,
                                discriminator
                            )
                        )
                else:
                    serialized_data[self.discriminator_field_name] = discriminator
                return serialized_data
        raise ConstraintException(
            tuple(path),
            "Invalid type: '{}', expected one of '{}'".format(
                type(data).__name__,
                "', '".join(types)
            )
        )


class ScopeType(ScopeSchema, AbstractType):
    """
    A scope is a container object for an object structure. Its main purpose is to hold objects that can be referenced,
    even in cases where circular references are desired.
    """

    def __init__(
            self,
            root: str,
            objects: Dict[
                ID_TYPE,
                ObjectType
            ],
    ):
        super().__init__(root, objects)

    def unserialize(self, data: Any, path: typing.Tuple[str] = tuple([])) -> TypeT:
        root_object: ObjectType = self.objects[self.root]
        new_path = list(path)
        new_path.append(root_object.__name__)
        return root_object.unserialize(data, tuple(new_path))

    def validate(self, data: TypeT, path: typing.Tuple[str] = tuple([])):
        root_object: ObjectType = self.objects[self.root]
        new_path = list(path)
        new_path.append(root_object.__name__)
        return root_object.validate(data, tuple(new_path))

    def serialize(self, data: TypeT, path: typing.Tuple[str] = tuple([])) -> Any:
        root_object: ObjectType = self.objects[self.root]
        new_path = list(path)
        new_path.append(root_object.__name__)
        return root_object.serialize(data, tuple(new_path))


class RefType(RefSchema, AbstractType):
    """
    A ref is a reference to an object in a Scope.
    """
    _scope: ScopeType

    def __init__(self, id: str, scope: ScopeType):
        super().__init__(id)
        self._scope = scope

    def unserialize(self, data: Any, path: typing.Tuple[str] = tuple([])) -> TypeT:
        return self._scope.objects[self.id].unserialize(data, path)

    def validate(self, data: TypeT, path: typing.Tuple[str] = tuple([])):
        return self._scope.objects[self.id].validate(data, path)

    def serialize(self, data: TypeT, path: typing.Tuple[str] = tuple([])) -> Any:
        return self._scope.objects[self.id].serialize(data, path)


StepInputT = TypeVar("StepInputT", bound=object)
StepOutputT = TypeVar("StepOutputT", bound=object)


class StepType(StepSchema):
    """
    StepSchema describes the schema for a single step. The input is always one ObjectType, while there are multiple
    possible outputs identified by a string.
    """

    _handler: Callable[[StepInputT], typing.Tuple[str, StepOutputT]]

    def __init__(
            self,
            handler: Callable[[StepInputT], typing.Tuple[str, StepOutputT]],
            input: ScopeType,
            outputs: Dict[
                ID_TYPE,
                StepOutputSchema
            ],
            display: Optional[DisplayValue] = None
    ):
        super().__init__(input, outputs, display)
        self._handler = handler

    def __call__(
            self,
            params: StepInputT,
            skip_input_validation: bool = False,
            skip_output_validation: bool = False,
    ) -> typing.Tuple[str, StepOutputT]:
        """
        :param params: Input parameter for the step.
        :param skip_input_validation: Do not perform input data type validation. Use at your own risk.
        :param skip_output_validation: Do not validate returned output data. Use at your own risk.
        :return: The ID for the output datatype, and the output itself.
        """
        input: ScopeType = self.input
        if not skip_input_validation:
            input.validate(params, tuple(["input"]))
        result = self._handler(params)
        if len(result) != 2:
            raise BadArgumentException(
                "The step returned {} results instead of 2. Did your step return the correct results?".format(
                    len(result)
                )
            )
        output_id, output_data = result
        if output_id not in self.outputs:
            raise BadArgumentException(
                "The step returned an undeclared output ID: %s, please return one of: '%s'" % (
                    output_id,
                    "', '".join(self.outputs.keys())
                )
            )
        output: ScopeType = self.outputs[output_id]
        if not skip_output_validation:
            output.validate(output_data, tuple(["output", output_id]))
        return output_id, output_data


class SchemaType:
    """
    A schema is a definition of one or more steps that can be executed. The step has a defined input and output 
    """
    steps: Dict[str, StepType]

    def unserialize_input(self, step_id: str, data: Any) -> Any:
        """
        This function unserializes the input from a raw data to data structures, such as dataclasses. This function is
        automatically called by __call__ before running the step with the unserialized input.
        :param step_id: The step ID to use to look up the schema for unserialization.
        :param data: The raw data to unserialize.
        :return: The unserialized data in the structure the step expects it.
        """
        if step_id not in self.steps:
            raise NoSuchStepException(step_id)
        step = self.steps[step_id]
        return self._unserialize_input(step, data)

    @staticmethod
    def _unserialize_input(step: StepSchema, data: Any) -> Any:
        try:
            return step.input.unserialize(data)
        except ConstraintException as e:
            raise InvalidInputException(e) from e

    def call_step(self, step_id: str, input_param: Any) -> typing.Tuple[str, Any]:
        """
        This function calls a specific step with the input parameter that has already been unserialized. It expects the
        data to be already valid, use unserialize_input to produce a valid input. This function is automatically called
        by __call__ after unserializing the input.
        :param step_id: The ID of the input step to run.
        :param input_param: The unserialized data structure the step expects.
        :return: The ID of the output, and the data structure returned from the step.
        """
        if step_id not in self.steps:
            raise NoSuchStepException(step_id)
        step = self.steps[step_id]
        return self._call_step(step, input_param)

    @staticmethod
    def _call_step(
            step: StepSchema,
            input_param: Any,
            skip_input_validation: bool = False,
            skip_output_validation: bool = False,
    ) -> typing.Tuple[str, Any]:
        return step(
            input_param,
            skip_input_validation=skip_input_validation,
            skip_output_validation=skip_output_validation,
        )

    def serialize_output(self, step_id: str, output_id: str, output_data: Any) -> Any:
        """
        This function takes an output ID (e.g. "error") and structured output_data and serializes them into a format
        suitable for wire transport. This function is automatically called by __call__ after the step is run.
        :param step_id: The step ID to use to look up the schema for serialization.
        :param output_id: The string identifier for the output data structure.
        :param output_data: The data structure returned from the step.
        :return:
        """
        if step_id not in self.steps:
            raise NoSuchStepException(step_id)
        step = self.steps[step_id]
        return self._serialize_output(step, output_id, output_data)

    @staticmethod
    def _serialize_output(step, output_id: str, output_data: Any) -> Any:
        try:
            return step.outputs[output_id].serialize(output_data)
        except ConstraintException as e:
            raise InvalidOutputException(e) from e

    def __call__(self, step_id: str, data: Any, skip_serialization: bool = False) -> typing.Tuple[str, Any]:
        """
        This function takes the input data, unserializes it for the specified step, calls the specified step, and,
        unless skip_serialization is set, serializes the return data.
        :param step_id: the step to execute
        :param data: input data
        :param skip_serialization: skip result serialization to basic types
        :return: the result ID, and the resulting data in the structure matching the result ID
        """
        if step_id not in self.steps:
            raise NoSuchStepException(step_id)
        step = self.steps[step_id]
        input_param = self._unserialize_input(step, data)
        output_id, output_data = self._call_step(
            step,
            input_param,
            # Skip duplicate verification
            skip_input_validation=True,
            skip_output_validation=True,
        )
        if skip_serialization:
            step.outputs[output_id].validate(output_data)
            return output_id, output_data
        serialized_output_data = self._serialize_output(step, output_id, output_data)
        return output_id, serialized_output_data


# endregion

# region Build

class _SchemaBuilder:
    @classmethod
    def resolve(cls, t: any, scope: ScopeType) -> AbstractType:
        path: typing.List[str] = []
        if hasattr(t, "__name__"):
            path.append(t.__name__)

        return cls._resolve_abstract_type(t, tuple(path), scope)

    @classmethod
    def _resolve_abstract_type(
            cls,
            t: any,
            path: typing.Tuple[str],
            scope: ScopeType,
    ) -> AbstractType:
        result = cls._resolve(t, path, scope)
        if isinstance(result, PropertyType):
            res: PropertyType = result
            new_path = list(path)
            new_path.append(res.name)
            raise SchemaBuildException(
                tuple(new_path),
                "Unsupported attribute combination, you can only use typing.Optional, etc. in classes, but not in "
                "lists, dicts, etc." % res.name
            )
        res: AbstractType = result
        return res

    @classmethod
    def _resolve_field(cls, t: any, path: typing.Tuple[str], scope: ScopeType, ) -> PropertyType:
        result = cls._resolve(t, path, scope)
        if not isinstance(result, PropertyType):
            result = PropertyType(
                result
            )
        return result

    @classmethod
    def _resolve(
            cls,
            t: any,
            path: typing.Tuple[str],
            scope: ScopeType,
    ) -> typing.Union[AbstractType, PropertyType]:
        if isinstance(t, type):
            return cls._resolve_type(t, path, scope)
        elif isinstance(t, str):
            return cls._resolve_string(t, path, scope)
        elif isinstance(t, bool):
            return cls._resolve_bool(t, path, scope)
        elif isinstance(t, int):
            return cls._resolve_int(t, path, scope)
        elif isinstance(t, float):
            return cls._resolve_float(t, path, scope)
        elif isinstance(t, list):
            return cls._resolve_list(t, path, scope)
        elif isinstance(t, dict):
            return cls._resolve_dict(t, path, scope)
        elif typing.get_origin(t) == list:
            return cls._resolve_list_annotation(t, path, scope)
        elif typing.get_origin(t) == dict:
            return cls._resolve_dict_annotation(t, path, scope)
        elif typing.get_origin(t) == typing.Union:
            return cls._resolve_union(t, path, scope)
        elif typing.get_origin(t) == typing.Annotated:
            return cls._resolve_annotated(t, path, scope)
        else:
            raise SchemaBuildException(path, "Unable to resolve underlying type: %s" % type(t).__name__)

    @classmethod
    def _resolve_type(cls, t, path: typing.Tuple[str], scope: ScopeType):
        if issubclass(t, Enum):
            return cls._resolve_enum(t, path, scope)
        elif t == re.Pattern:
            return cls._resolve_pattern(t, path, scope)
        elif t == str:
            return cls._resolve_string_type(t, path, scope)
        elif t == bool:
            return cls._resolve_bool_type(t, path, scope)
        elif t == int:
            return cls._resolve_int_type(t, path, scope)
        elif t == float:
            return cls._resolve_float_type(t, path, scope)
        elif t == list:
            return cls._resolve_list_type(t, path, scope)
        elif typing.get_origin(t) == dict:
            return cls._resolve_dict_annotation(t, path, scope)
        elif t == dict:
            return cls._resolve_dict_type(t, path, scope)
        return cls._resolve_class(t, path, scope)

    @classmethod
    def _resolve_enum(
            cls,
            t,
            path: typing.Tuple[str],
            scope: ScopeType,
    ) -> AbstractType:
        try:
            return EnumType(
                t
            )
        except Exception as e:
            raise SchemaBuildException(path, "Constraint exception while creating enum type") from e

    @classmethod
    def _resolve_dataclass_field(
            cls,
            t: dataclasses.Field,
            path: typing.Tuple[str],
            scope: ScopeType,
    ) -> typing.Tuple[str, PropertyType]:
        underlying_type = cls._resolve_field(t.type, path, scope)
        if underlying_type.name == "":
            meta_name = t.metadata.get("name")
            if meta_name != "" and meta_name is not None:
                underlying_type.name = meta_name
            else:
                underlying_type.name = t.name
        meta_id = t.metadata.get("id")
        if meta_id is None:
            meta_id = t.name
        else:
            underlying_type.field_override = t.name
        meta_description = t.metadata.get("description")
        if meta_description != "" and meta_description is not None:
            underlying_type.description = meta_description
        if t.default != dataclasses.MISSING or t.default_factory != dataclasses.MISSING:
            underlying_type.required = False
        elif not underlying_type.required:
            raise SchemaBuildException(
                path,
                "Field is marked as optional, but does not have a default value set. "
                "Please set a default value for this field."
            )
        return meta_id, underlying_type

    @classmethod
    def _resolve_class(
            cls,
            t,
            path: typing.Tuple[str],
            scope: ScopeType,
    ) -> AbstractType:
        final_fields: Dict[str, PropertyType] = {}

        try:
            fields_list = dataclasses.fields(t)
        except TypeError as e:
            unsupported_types = {
                tuple: "tuples",
                complex: "complex numbers",
                bytes: "bytes",
                bytearray: "bytearrays",
                range: "banges",
                memoryview: "memoryviews",
                set: "sets",
                frozenset: "frozensets",
                GenericAlias: "generic aliases",
                types.ModuleType: "modules",
            }
            for unsupported_type, unsupported_type_name in unsupported_types.items():
                if isinstance(t, unsupported_type) or t == unsupported_type:
                    raise SchemaBuildException(
                        path,
                        "{} are not supported by the Arcaflow typing system and cannot be used in input or output data"
                        "types. Please use one of the supported types, or file an issue at {} with your use case to "
                        "get them included.".format(
                            unsupported_type_name,
                            _issue_url
                        )
                    )
            raise SchemaBuildException(
                path,
                "{} is not a dataclass or a supported type. Please use the @dataclasses.dataclass decorator on your "
                "class or use a supported native type. If this is a native Python type and you want to request support "
                "for it in the Arcaflow SDK, please open an issue at {} to get it included.".format(
                    t.__name__,
                    _issue_url
                ),
            ) from e

        for f in fields_list:
            new_path = list(path)
            new_path.append(f.name)
            name, final_field = cls._resolve_dataclass_field(f, tuple(new_path), scope)
            final_fields[name] = final_field

        try:
            scope.objects[t.__name__] = ObjectType(
                t,
                final_fields,
            )

            return RefType(t.__name__, scope)
        except Exception as e:
            raise SchemaBuildException(path, "Failed to create object type: {}".format(e.__str__())) from e

    @classmethod
    def _resolve_bool_type(
            cls,
            t,
            path: typing.Tuple[str],
            scope: ScopeType,
    ) -> BoolType:
        try:
            return BoolType()
        except Exception as e:
            raise SchemaBuildException(path, "Constraint exception while creating bool type") from e

    @classmethod
    def _resolve_bool(
            cls,
            t,
            path: typing.Tuple[str],
            scope: ScopeType,
    ) -> BoolType:
        try:
            return BoolType()
        except Exception as e:
            raise SchemaBuildException(path, "Constraint exception while creating bool type") from e

    @classmethod
    def _resolve_string_type(
            cls,
            t,
            path: typing.Tuple[str],
            scope: ScopeType,
    ) -> StringType:
        try:
            return StringType()
        except Exception as e:
            raise SchemaBuildException(path, "Constraint exception while creating string type") from e

    @classmethod
    def _resolve_string(
            cls,
            t,
            path: typing.Tuple[str],
            scope: ScopeType,
    ) -> StringType:
        try:
            return StringType()
        except Exception as e:
            raise SchemaBuildException(path, "Constraint exception while creating string type") from e

    @classmethod
    def _resolve_int(
            cls,
            t,
            path: typing.Tuple[str],
            scope: ScopeType,
    ) -> IntType:
        try:
            return IntType()
        except Exception as e:
            raise SchemaBuildException(path, "Constraint exception while creating int type") from e

    @classmethod
    def _resolve_int_type(
            cls,
            t,
            path: typing.Tuple[str],
            scope: ScopeType,
    ) -> IntType:
        try:
            return IntType()
        except Exception as e:
            raise SchemaBuildException(path, "Constraint exception while creating int type") from e

    @classmethod
    def _resolve_float(
            cls,
            t,
            path: typing.Tuple[str],
            scope: ScopeType,
    ) -> FloatType:
        try:
            return FloatType()
        except Exception as e:
            raise SchemaBuildException(path, "Constraint exception while creating float type") from e

    @classmethod
    def _resolve_float_type(
            cls,
            t,
            path: typing.Tuple[str],
            scope: ScopeType,
    ) -> FloatType:
        try:
            return FloatType()
        except Exception as e:
            raise SchemaBuildException(path, "Constraint exception while creating float type") from e

    @classmethod
    def _resolve_annotated(
            cls,
            t,
            path: typing.Tuple[str],
            scope: ScopeType,
    ):
        args = typing.get_args(t)
        if len(args) < 2:
            raise SchemaBuildException(
                path,
                "At least one validation parameter required for typing.Annotated"
            )
        new_path = list(path)
        new_path.append("typing.Annotated")
        path = tuple(new_path)
        underlying_t = cls._resolve(args[0], path, scope)
        for i in range(1, len(args)):
            new_path = list(path)
            new_path.append(str(i))
            if not isinstance(args[i], typing.Callable):
                raise SchemaBuildException(tuple(new_path), "Annotation is not callable")
            try:
                underlying_t = args[i](underlying_t)
            except Exception as e:
                raise SchemaBuildException(
                    tuple(new_path),
                    "Failed to execute Annotated argument: {}".format(e.__str__()),
                ) from e
        return underlying_t

    @classmethod
    def _resolve_list(
            cls,
            t,
            path: typing.Tuple[str],
            scope: ScopeType,
    ) -> AbstractType:
        raise SchemaBuildException(
            path,
            "List type without item type definition encountered, please declare your lists like this: "
            "typing.List[str]"
        )

    @classmethod
    def _resolve_list_type(
            cls,
            t,
            path: typing.Tuple[str],
            scope: ScopeType,
    ) -> AbstractType:
        raise SchemaBuildException(
            path,
            "List type without item type definition encountered, please declare your lists like this: "
            "typing.List[str]"
        )

    @classmethod
    def _resolve_list_annotation(
            cls,
            t,
            path: typing.Tuple[str],
            scope: ScopeType,
    ):
        args = get_args(t)
        if len(args) != 1:
            raise SchemaBuildException(
                path,
                "List type without item type definition encountered, please declare your lists like this: "
                "typing.List[str]"
            )
        new_path = list(path)
        new_path.append("items")
        try:
            return ListType(
                cls._resolve_abstract_type(args[0], tuple(new_path), scope)
            )
        except Exception as e:
            raise SchemaBuildException(path, "Failed to create list type") from e

    @classmethod
    def _resolve_dict(
            cls,
            t,
            path: typing.Tuple[str],
            scope: ScopeType,
    ) -> AbstractType:
        raise SchemaBuildException(
            path,
            "Dict type without item type definition encountered, please declare your dicts like this: "
            "typing.Dict[str, int]"
        )

    @classmethod
    def _resolve_dict_type(
            cls,
            t,
            path: typing.Tuple[str],
            scope: ScopeType,
    ) -> AbstractType:
        raise SchemaBuildException(
            path,
            "Dict type without item type definition encountered, please declare your dicts like this: "
            "typing.Dict[str, int]"
        )

    @classmethod
    def _resolve_dict_annotation(
            cls,
            t,
            path: typing.Tuple[str],
            scope: ScopeType,
    ):
        args = typing.get_args(t)
        if len(args) != 2:
            raise SchemaBuildException(
                path,
                "Dict type without item type definition encountered, please declare your dicts like this: "
                "typing.Dict[str, int]"
            )
        keys_path = list(path)
        keys_path.append("keys")
        key_schema: AbstractType = cls._resolve_abstract_type(args[0], tuple(keys_path), scope)

        values_path = list(path)
        values_path.append("values")
        value_schema = cls._resolve_abstract_type(args[1], tuple(values_path), scope)

        try:
            return MapType(
                key_schema,
                value_schema,
            )
        except Exception as e:
            raise SchemaBuildException(path, "Failed to create map type") from e

    @classmethod
    def _resolve_union(cls, t, path: typing.Tuple[str], scope: ScopeType, ) -> OneOfType:
        args = typing.get_args(t)
        try:
            if isinstance(None, args[0]):
                raise SchemaBuildException(path, "None types are not supported.")
        except TypeError:
            pass
        try:
            if isinstance(None, args[1]):
                new_path = list(path)
                new_path.append("typing.Optional")
                result = cls._resolve_field(args[0], tuple(path), scope)
                result.required = False
                return result
        except TypeError:
            pass
        result = OneOfType(
            "_type",
            StringType(),
            {}
        )
        for i in range(len(args)):
            new_path = list(path)
            new_path.append("typing.Union")
            new_path.append(str(i))
            f = cls._resolve_field(args[i], tuple(new_path))
            if not f.required:
                raise SchemaBuildException(
                    tuple(new_path),
                    "Union types cannot contain optional values."
                )
            if len(f.required_if) != 0:
                raise SchemaBuildException(
                    tuple(new_path),
                    "Union types cannot simultaneously contain require_if fields"
                )
            if len(f.required_if_not) != 0:
                raise SchemaBuildException(
                    tuple(new_path),
                    "Union types cannot simultaneously contain require_if_not fields"
                )
            if len(f.conflicts) != 0:
                raise SchemaBuildException(
                    tuple(new_path),
                    "Union types cannot simultaneously contain conflicts fields"
                )
            if f.type.type_id() != TypeID.OBJECT:
                raise SchemaBuildException(
                    tuple(new_path),
                    "Union types can only contain objects, {} found".format(f.type.type_id())
                )
            t: ObjectType = f.type
            result.one_of[t.type_class().__name__] = t
        return result

    @classmethod
    def _resolve_pattern(cls, t, path, scope: ScopeType, ):
        try:
            return PatternType()
        except Exception as e:
            raise SchemaBuildException(path, "Failed to create pattern type") from e


def build_object_schema(t, skip_validation: bool = False) -> ScopeType:
    """
    This function builds a schema for a single object. This is useful when serializing input parameters into a file
    for underlying tools to use, or unserializing responses from underlying tools into output data types.

    :param t: the type to build a schema for.
    :return: the built object schema
    """
    scope = ScopeType(t.__name__, {})

    r = _SchemaBuilder.resolve(t, scope)
    if not isinstance(r, ObjectSchema):
        raise SchemaBuildException(tuple([]), "Response type is not an object.")

    if not skip_validation:
        _scope_schema.validate(scope)

    return scope


# Build a schema for the scope, then for the entire schema for validation.
_scope_schema = build_object_schema(ScopeType, True)
_schema_schema = build_object_schema(Schema)

# endregion
