import dataclasses
import inspect
import io
import json
import pprint
import sys
import traceback
import typing

import yaml
from sys import argv, stdin, stdout, stderr
from optparse import OptionParser
from typing import List, Callable, TypeVar, Dict, Type

from arcaflow_plugin_sdk import schema, serialization, jsonschema
from arcaflow_plugin_sdk.schemabuilder import SchemaBuilder
from arcaflow_plugin_sdk.schema import BadArgumentException, InvalidInputException, InvalidOutputException

_issue_url = "https://github.com/arcalot/arcaflow-plugin-sdk-python/issues"

InputT = TypeVar("InputT")
OutputT = TypeVar("OutputT")

_step_decorator_param = Callable[[InputT], OutputT]


def step(
        id: str,
        name: str,
        description: str,
        outputs: Dict[str, Type]
) -> Callable[
    [_step_decorator_param],
    schema.StepSchema[InputT]
]:
    """
    @plugin.step is a decorator that takes a function with a single parameter and creates a schema for it that you can
    use with plugin.build_schema.

    :param id: The identifier for the step.
    :param name: The human-readable name for the step.
    :param description: The human-readable description for the step.
    :param outputs: A dict linking response IDs to response object types.
    :return: A schema for the step.
    """

    def step_decorator(func: _step_decorator_param) -> schema.StepSchema[InputT]:
        if id == "":
            raise BadArgumentException("Steps cannot have an empty ID")
        if name == "":
            raise BadArgumentException("Steps cannot have an empty name")
        sig = inspect.signature(func)
        if len(sig.parameters) != 1:
            raise BadArgumentException("The '%s' (id: %s) step must have exactly one parameter" % (name, id))
        input_param = list(sig.parameters.values())[0]
        if input_param.annotation is inspect.Parameter.empty:
            raise BadArgumentException("The '%s' (id: %s) step parameter must have a type annotation" % (name, id))
        if isinstance(input_param.annotation, str):
            raise BadArgumentException("Stringized type annotation encountered in %s (id: %s). Please make sure you "
                                       "don't import annotations from __future__ to avoid this problem." % (name, id))

        new_responses: Dict[str, schema.ObjectType] = {}
        for response_id in list(outputs.keys()):
            new_responses[response_id] = build_object_schema(outputs[response_id])

        return schema.StepSchema(
            id,
            name,
            description,
            input=build_object_schema(input_param.annotation),
            outputs=new_responses,
            handler=func,
        )

    return step_decorator


class _ExitException(Exception):
    def __init__(self, exit_code: int, msg: str):
        self.exit_code = exit_code
        self.msg = msg


class _CustomOptionParser(OptionParser):
    def error(self, msg):
        raise _ExitException(2, msg + "\n" + self.get_usage())


SchemaBuildException = schema.SchemaBuildException
build_object_schema = schema.build_object_schema


def run(
        s: schema.Schema,
        argv: List[str] = tuple(argv),
        stdin: io.TextIOWrapper = stdin,
        stdout: io.TextIOWrapper = stdout,
        stderr: io.TextIOWrapper = stderr
) -> int:
    """
    Run takes a schema and runs it as a command line utility. It returns the exit code of the program. It is intended
    to be used as an entry point for your plugin.
    :param s: the schema to run
    :param argv: command line arguments
    :param stdin: standard input
    :param stdout: standard output
    :param stderr: standard error
    :return: exit code
    """
    try:
        parser = _CustomOptionParser()
        parser.add_option(
            "-f",
            "--file",
            dest="filename",
            help="Configuration file to read configuration from. Pass - to read from stdin.",
            metavar="FILE",
        )
        parser.add_option(
            "--json-schema",
            dest="json_schema",
            help="Print JSON schema for either the input or the output.",
            metavar="KIND",
        )
        parser.add_option(
            "-s",
            "--step",
            dest="step",
            help="Which step to run? One of: " + ', '.join(s.steps.keys()),
            metavar="STEPID",
        )
        parser.add_option(
            "-d",
            "--debug",
            action="store_true",
            dest="debug",
            help="Enable debug mode (print step output and stack traces)."
        )
        (options, remaining_args) = parser.parse_args(list(argv[1:]))
        if len(remaining_args) > 0:
            raise _ExitException(
                64,
                "Unable to parse arguments: [" + ', '.join(remaining_args) + "]\n" + parser.get_usage()
            )
        if len(s.steps) > 1 and options.step is None:
            raise _ExitException(64, "-s|--step is required\n" + parser.get_usage())
        if options.step is not None:
            step_id = options.step
        else:
            step_id = list(s.steps.keys())[0]
        if options.filename is not None:
            return _execute_file(step_id, s, options, stdin, stdout, stderr)
        elif options.json_schema is not None:
            return _print_json_schema(step_id, s, options, stdout)
        else:
            raise _ExitException(
                64,
                "one of -f|--filename or --json-schema is required\n{}".format(parser.get_usage()),
            )
    except serialization.LoadFromFileException as e:
        stderr.write(e.msg + '\n')
        return 64
    except _ExitException as e:
        stderr.write(e.msg + '\n')
        return e.exit_code


def build_schema(*args: schema.StepSchema) -> schema.Schema:
    """
    This function takes functions annotated with @plugin.step and creates a schema from them.
    :param args: the steps to be added to the schema
    :return: a callable schema
    """
    steps_by_id: Dict[str, schema.StepSchema] = {}
    for step in args:
        if step.id in steps_by_id:
            raise BadArgumentException("Duplicate step ID %s" % step.id)
        steps_by_id[step.id] = step
    return schema.Schema(
        steps_by_id
    )


def _execute_file(
        step_id: str,
        s: schema.StepSchema,
        options,
        stdin: io.TextIOWrapper,
        stdout: io.TextIOWrapper,
        stderr: io.TextIOWrapper
) -> int:
    filename: str = options.filename
    if filename == "-":
        data = serialization.load_from_stdin(stdin)
    else:
        data = serialization.load_from_file(filename)
    original_stdout = sys.stdout
    original_stderr = sys.stderr
    if options.debug:
        # Redirect stdout to stderr for debug logging
        sys.stdout = stderr
        sys.stderr = stderr
    else:
        out_buffer = io.StringIO()
        sys.stdout = out_buffer
        sys.stderr = out_buffer
    try:
        output_id, output_data = s(step_id, data)
        output = {
            "output_id": output_id,
            "output_data": output_data
        }
        stdout.write(yaml.dump(output, sort_keys=False))
        return 0
    except InvalidInputException as e:
        stderr.write(
            "Invalid input encountered while executing step '{}' from file '{}':\n  {}\n\n".format(
                step_id,
                filename,
                e.__str__()
            )
        )
        if options.debug:
            traceback.print_exc(chain=True)
        else:
            stderr.write("Set --debug to print a stack trace.")
        return 65
    except InvalidOutputException as e:
        stderr.write(
            "Bug: invalid output encountered while executing step '{}' from file '{}':\n  {}\n\n".format(
                step_id,
                filename,
                e.__str__()
            )
        )
        if options.debug:
            traceback.print_exc(chain=True)
        else:
            stderr.write("Set --debug to print a stack trace.")
        return 70
    finally:
        sys.stdout = original_stdout
        sys.stderr = original_stderr


def _print_json_schema(step_id, s, options, stdout):
    if options.json_schema == "input":
        data = jsonschema.step_input(s.steps[step_id])
    elif options.json_schema == "output":
        data = jsonschema.step_outputs(s.steps[step_id])
    else:
        raise _ExitException(64, "--json-schema must be one of 'input' or 'output'")
    stdout.write(json.dumps(data, indent="  "))
    return 0


def test_object_serialization(
        dc,
        fail: typing.Optional[Callable[[str], None]] = None,
        t: typing.Optional[schema.ObjectType] = None
):
    """
    This function aids serialization by first serializing, then unserializing the passed parameter according to the
    passed schema. It then compares that the two objects are equal.
    :param dc: the dataclass to use for tests.
    :param t: the schema for the dataclass. If none is passed, the schema is built automatically using
    plugin.build_object_schema()
    """
    try:
        if t is None:
            t = build_object_schema(dc.__class__)
        path: typing.Tuple[str] = tuple([dc.__class__.__name__])
        t.validate(dc, path)
        serialized_data = t.serialize(dc, path)
        unserialized_data = t.unserialize(serialized_data, path)
        if unserialized_data != dc:
            raise Exception(
                "After serializing and unserializing {}, the data mismatched. Serialized data was: {}".format(
                    dc.__name__,
                    serialized_data
                )
            )
    except Exception as e:
        result = "Your object serialization test for {} failed.\n\n" \
                 "This means that your object cannot be properly serialized by the SDK. There are three possible " \
                 "reasons for this:\n\n" \
                 "1. Your has a field type in it that the SDK doesn't support\n" \
                 "2. Your sample data is invalid according to your own rules\n" \
                 "3. There is a bug in the SDK (please report it)\n\n" \
                 "Check the error message below for details.\n\n" \
                 "---\n\n".format(type(dc).__name__, traceback.extract_stack())
        result += "Error message:\n" + e.__str__() + "\n\n"
        result += "Input:\n" + pprint.pformat(dataclasses.asdict(dc)) + "\n\n"
        result += "---\n\n"
        result += "Your object serialization test for {} failed. Please scroll up for details.\n\n".format(
            type(dc).__name__
        )
        if fail is None:
            print(result)
            sys.exit(1)
        fail(result)
