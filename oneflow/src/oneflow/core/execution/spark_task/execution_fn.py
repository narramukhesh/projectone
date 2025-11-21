from oneflow.core.types import F
from typing import Type, Tuple, Any, Dict
import inspect
from oneflow.core.exception.execution import ExecutionFuncInitializeError
import importlib


class SparkExecutionFunction:
    """This class implementation holds the Spark Execution Function"""

    def __init__(
        self,
        func_name: str = None,
        func_body: str = None,
        func_obj: Type[F] = None,
        func_module: str = None,
        func_file: str = None,
        extra_arguments: Dict[str, Any] = None,
    ):
        """
        This is a initilization method which initialized the spark execution function

        following any parameters can be provided
        Parameters
        -------------
        func_name: str
            function name which resolves to function object to help for function body

        func_body: str
            function body string which will be evaluated and placed with execution

        func_obj: Type[F]
            passing the direct function object

        func_module: str
            function module to get the function object can be specified as `module.func_name`

        func_file: str
            function source file to get the function object

        extra_arguments: Dict[str,Any]
            function extra arguments to be passed to the execution transformation function

        """
        function_obj = None
        if func_module is not None:
            function_obj = self.__get_function_obj_module(
                func_module=func_module, func_name=func_name
            )
        elif func_obj is not None:
            function_obj = func_obj
        elif (func_body is not None) and (func_name is not None):
            function_obj = self.__get_function_obj_body(
                func_name=func_name, func_body=func_body
            )
        elif (func_file is not None) and (func_name is not None):
            function_obj = self.__get_function_obj_file(
                func_name=func_name, func_file=func_file
            )
        else:
            raise ExecutionFuncInitializeError(
                "Provided initialization arguments for execution function are not supported. Either func_module should be given or func_obj should be defined or both func_body and func_name should be defined"
            )
        self.execution_function = function_obj

        self.__set_function_arguments(extra_arguments)

    def __set_function_arguments(self, arguments=None):
        """This is initialized method to get the function object arguments"""
        try:
            func_args = inspect.getfullargspec(self.execution_function)
            if len(func_args.args) == 0:
                raise ExecutionFuncInitializeError(
                    "Error caused by inspecting the arguments for the function object where there is no positional argument, without them inputs can't be passed. Please check the execution function arguments"
                )
            self.execution_function_args = func_args.args
            self.execution_function_args_defaults = {}
            self.execution_function_kwargs_defaults = {}
            var_default_map = {}
            kwvar_default_map = {}
            if (arguments is not None) and isinstance(arguments, dict):
                required_map_args = func_args.args + func_args.kwonlyargs
                if (
                    len(set(arguments.keys()) - set(required_map_args)) > 0
                    and func_args.varkw is None
                ):
                    raise ExecutionFuncInitializeError(
                        "Error caused by inspecting the extra arguments passed in configuration, keyword argument is not defined which is not able to map extra argument"
                    )
                for k in arguments:
                    if k in func_args.args:
                        var_default_map[k] = arguments[k]
                    else:
                        kwvar_default_map[k] = arguments[k]
            if (func_args.kwonlydefaults is not None) and isinstance(func_args, dict):
                kwvar_default_map = {
                    **func_args.kwonlydefaults,
                    **kwvar_default_map,
                }
            if func_args.defaults is not None:
                defaults_var_names = func_args.args[-len(func_args.defaults) :]
                var_default_map = {
                    **dict(zip(defaults_var_names, func_args.defaults)),
                    **var_default_map,
                }

            self.execution_function_args_defaults = var_default_map
            self.execution_function_kwargs_defaults = kwvar_default_map

        except ExecutionFuncInitializeError as e:
            raise ExecutionFuncInitializeError(e)
        except Exception as e:
            raise ExecutionFuncInitializeError(
                f"Error caused by inspecting the arguments for the function object because of the error {e}"
            )

    def __get_function_obj_module(
        self, func_module: str, func_name: str = None
    ) -> Tuple[Any]:
        """This is initialized method to get the function object module"""
        if func_name is None:
            if (
                isinstance(func_module, str)
                and len(func_module.split(".")) <= 2
                and func_module.split(".")[-1] == ""
            ):
                raise ExecutionFuncInitializeError(
                    "Provided function module is incorrect format should be in format `module.function`"
                )

            parent_module = ".".join(func_module.split(".")[:-1])
            function_name = func_module.split(".")[-1]
        else:
            parent_module = func_module
            function_name = func_name

        try:
            func_obj = getattr(importlib.import_module(parent_module), function_name)
            if not callable(func_obj):
                raise ExecutionFuncInitializeError(
                    f"Provided function `{func_module}` is not a callable function"
                )
            else:
                return func_obj
        except ModuleNotFoundError:
            raise ExecutionFuncInitializeError(
                f"Provided function module `{parent_module}` cannot be resolved"
            )
        except AttributeError:
            raise ExecutionFuncInitializeError(
                f"Provided function `{func_module}` cannot be resolved"
            )
        except NameError:
            raise ExecutionFuncInitializeError(
                f"Error caused by inspecting the arguments for the function object `{func_module}`"
            )

    def __get_function_obj_file(self, func_file: str, func_name: str) -> Tuple[Any]:
        """This is initialized method to get the function object from file"""

        try:
            spec = importlib.util.spec_from_file_location(
                "custom_module.execution_function", func_file
            )
            func_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(func_module)
            func_obj = getattr(func_module, func_name)
            if not callable(func_obj):
                raise ExecutionFuncInitializeError(
                    f"Provided function `{func_module}` is not a callable function"
                )
            else:
                return func_obj

        except Exception as e:
            raise ExecutionFuncInitializeError(
                f"Error caused by inspecting function object file, It can be invalid python file or check the extention of file it should be .py error caused by {e}"
            )

    def __get_function_obj_body(self, func_name: str, func_body: str) -> Tuple[Any]:
        """This is initialized method to get the function object from  body to get the from function name"""
        try:
            function_definition = {}
            exec(func_body, globals(), function_definition)
            if not callable(function_definition.get(func_name, None)):
                raise ExecutionFuncInitializeError(
                    f"Error Provided function name `{func_name}` is not matching with function definition"
                )
            func_obj = function_definition[func_name]
            return func_obj
        except Exception as e:
            raise ExecutionFuncInitializeError(
                f"Error caused by inspecting the function body to function object, Please check execution function syntax for error {e}"
            )
        except ExecutionFuncInitializeError as e:
            raise ExecutionFuncInitializeError(e)
