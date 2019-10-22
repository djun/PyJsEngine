# coding=utf-8


import subprocess
import types
import traceback as tb
import csv
from threading import Lock
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from time import strftime, strptime, time, localtime, sleep
from copy import deepcopy
from collections import ChainMap

from jinja2 import Environment, ChoiceLoader, FileSystemLoader, Template

from PyJsEngineBase import PyJsEngineBase
from MiniUtils import *

__version__ = "1.0.191022"


class PyJsEngine(PyJsEngineBase):
    tagn_root = "root"
    tagn_sub = "sub"
    tagn_loop = "loop"
    tagn_for_in = "for_in"  # new
    tagn_if = "if"  # new
    tagn_elif = "elif"  # new
    tagn_then = "then"  # new
    tagn_else = "else"  # new
    tagn_case = "case"  # new
    tagn_when = "when"  # new
    tagn_when_others = "when_others"  # new
    tagn_assert = "assert"
    tagn_assert_not = "assert_not"
    tagn_assert_in = "assert_in"  # new
    tagn_assert_not_in = "assert_not_in"  # new
    tagn_not = "not"
    tagn_equal = "equal"  # new
    tagn_not_equal = "not_equal"  # new
    tagn_less = "less"  # new
    tagn_less_equal = "less_equal"  # new
    tagn_greater = "greater"  # new
    tagn_greater_equal = "greater_equal"  # new
    tagn_and = "and"  # new
    tagn_or = "or"  # new
    tagn_module = "module"
    tagn_procedure = "procedure"
    tagn_procedure_privilege = "procedure_privilege"  # new: for cond_call
    tagn_call = "call"
    tagn_cond_call = "cond_call"  # new: for cond_call
    tagn_return = "return"  # new
    tagn_set = "set"
    tagn_set_int = "set_int"  # new
    tagn_set_num = "set_num"  # new
    tagn_set_str = "set_str"  # new
    tagn_set_eval = "set_eval"  # new
    tagn_get = "get"  # new
    # tagn_method = "method"  # new
    # tagn_property = "property"  # new
    tagn_load_vars = "load_vars"  # new
    tagn_load_data = "load_data"
    # tagn_save_data = "save_data"
    tagn_output = "output"  # new
    tagn_iter_make = "iter_make"
    tagn_iter_next = "iter_next"
    tagn_handle_exceptions = "handle_exceptions"
    tagn_json_load = "json_load"  # new for json
    tagn_json_dump = "json_dump"  # new for json
    tagn_template = "template"  # new for jinja2
    tagn_fopen = "fopen"  # new
    tagn__sleep = "_sleep"  # new
    tagn__json = "_json"  # new
    tagn__csv = "_csv"  # new
    tagn_call_os_cmd = "call_os_cmd"  # new, high privilege

    tagns_high_privilege_set = {tagn_call_os_cmd, tagn_procedure_privilege}

    # attrn_abort_on_err = "abort_on_err"
    attrn_name = "name"
    attrn_src = "src"
    attrn_key = "key"
    attrn_key_eval = "key_eval"
    attrn_value = "value"
    attrn_default_value = "defvalue"
    attrn_index = "index"
    attrn_dict_key = "dict_key"
    attrn_key1 = "key1"
    attrn_key2 = "key2"
    attrn_type = "type"
    attrn_cols = "cols"
    attrn_global = "global"
    attrn_pass_through = "pass_through"
    attrn_debug = "debug"
    attrn_auto_strip = "auto_strip"
    attrn_allow_none = "allow_none"
    attrn_mode = "mode"
    attrn_newline = "newline"
    attrn_encoding = "encoding"
    attrn_content = "content"  # new
    attrn__from_file = "_from_file"  # new
    attrn__to_file = "_to_file"  # new
    attrn__to_key = "_to_key"  # new
    attrn__prop = "_prop"  # new
    attrn__args = "_args"  # new
    attrn__assert = "_assert"  # new

    REF_KEY_PREFIX = "__"
    SUB_SPLITTER = "."

    SET_TYPE_VALUE = "value"
    SET_TYPE_EVAL = "eval"
    SET_TYPE_OBJECT = "object"

    FILE_TYPE_CSV = "csv"

    PROC_SWITCHER_PREFIX = "!"
    PROC_SWITCHER_FLAG = "1"

    MVAR_LOADED_DATA_COUNT = "__loaded_data_count__"
    MVAR_LOADED_DATA_ITEM_INDEX = "__loaded_data_item_index__"
    MVAR_LOADED_DATA_COLS = "__loaded_data_cols__"
    MVAR_EXCEPTIONS_HANDLER = "__exceptions_handler__"
    MVAR_DEBUG = "__debug__"
    MVAR_DEPTH = "__depth__"
    MVAR_LOG_DATETIME = "__log_datetime__"
    MVAR_FOR_IN_KEY = "__for_in_key__"
    MVAR_FOR_IN_VALUE = "__for_in_value__"
    MVAR_PROC_PRIVILEGE = "__proc_privilege__"

    def __init__(self, logger=None, args=None):
        # 父类初始化
        PyJsEngineBase.__init__(self, logger, args)
        # 更新MVAR集合
        self.MVAR_SET.update({
            self.MVAR_LOADED_DATA_COUNT,
            self.MVAR_LOADED_DATA_ITEM_INDEX,
            self.MVAR_LOADED_DATA_COLS,
            self.MVAR_EXCEPTIONS_HANDLER,
            self.MVAR_DEBUG,
            self.MVAR_DEPTH,
            self.MVAR_LOG_DATETIME,
            self.MVAR_FOR_IN_KEY,
            self.MVAR_FOR_IN_VALUE,
            self.MVAR_PROC_PRIVILEGE,
        })
        # 用于存放过程
        self._proc_dict = dict()
        # 其他变量
        self._encoding = self.DEFAULT_ENCODING

        # 2019-5-5
        self._j2_env = None  # Jinja2 environment

        # 2019-9-21
        self._output_lmap = {}
        self._output_lmap_lock = Lock()

        self.register_context({
            self.tagn_root: self.run_root,
            self.tagn_sub: self.run_sub,
            self.tagn_loop: self.run_loop,
            self.tagn_for_in: self.run_for_in,
            self.tagn_if: self.run_if_elif,
            self.tagn_elif: self.run_if_elif,
            self.tagn_then: self.run_then_else,
            self.tagn_else: self.run_then_else,
            self.tagn_case: self.run_case,
            self.tagn_when: self.run_when,
            self.tagn_when_others: self.run_when,
            self.tagn_assert: self.run_assert,
            self.tagn_assert_not: self.run_assert_not,
            self.tagn_assert_in: self.run_assert_in,
            self.tagn_assert_not_in: self.run_assert_not_in,
            self.tagn_not: self.run_not,
            self.tagn_equal: self.run_logical_decision,
            self.tagn_not_equal: self.run_logical_decision,
            self.tagn_less: self.run_logical_decision,
            self.tagn_less_equal: self.run_logical_decision,
            self.tagn_greater: self.run_logical_decision,
            self.tagn_greater_equal: self.run_logical_decision,
            self.tagn_and: self.run_complex_condition,
            self.tagn_or: self.run_complex_condition,
            self.tagn_module: self.run_module,
            self.tagn_procedure: self.run_procedure,
            self.tagn_call: self.run_call,
            self.tagn_cond_call: self.run_cond_call,
            self.tagn_return: self.run_get,
            self.tagn_set: self.run_set,
            self.tagn_set_int: self.run_set_with_type,
            self.tagn_set_num: self.run_set_with_type,
            self.tagn_set_str: self.run_set_with_type,
            self.tagn_set_eval: self.run_set_with_type,
            self.tagn_get: self.run_get,
            # self.tagn_method: self.run_method,
            # self.tagn_property: self.run_property,
            self.tagn_load_vars: self.run_load_vars,
            self.tagn_load_data: self.run_load_data,
            # self.tagn_save_data: self.run_save_data,
            self.tagn_output: self.run_output,
            self.tagn_iter_make: self.run_iter_make,
            self.tagn_iter_next: self.run_iter_next,
            self.tagn_handle_exceptions: self.run_handle_exceptions,
            self.tagn_json_load: self.run_json_load,
            self.tagn_json_dump: self.run_json_dump,
            self.tagn_template: self.run_template,
            self.tagn_call_os_cmd: self.run_call_os_cmd,
            # --------
            self.tagn__sleep: sleep,
            self.tagn__json: json,
            self.tagn__csv: csv,
            self.tagn_fopen: open,
            "len": len,
            "datetime": datetime,
            "timezone": timezone,
            "relativedelta": relativedelta,
            "time": time,
            "localtime": localtime,
            "strftime": strftime,
            "strptime": strptime,
        })
        # self._logger.debug(msg="registed runners={}".format(repr(self._tag_runners_map)))  # debug

        self._logger.debug(msg="PyJsEngine loaded. ({})".format(__version__))

    @property
    def encoding(self):
        return self._encoding

    def init_jinja2_env(self):
        """
        为Jinja2模板功能初始化一个Environment（使用FileSystemLoader加载器 从_path中的路径依次查找模板）
        """

        logger = self._logger
        if isinstance(self._path, list):
            self._j2_env = None
            try:
                loaders = [
                    FileSystemLoader(self._path),
                ]
                cloader = ChoiceLoader(loaders)

                self._j2_env = Environment(loader=cloader)
                logger.debug(msg="[PyJsEngine]<init_jinja2_env>: Jinja2 Environment initialized.")
            except Exception as e:
                self._j2_env = None
                logger.error(msg="[PyJsEngine]<init_jinja2_env>: {}".format(str(e)))
                logger.debug(msg="------Traceback------\n" + tb.format_exc())

    # 加载模块脚本
    def load_module(self, source):
        logger = self._logger
        logger.debug(msg="Loading module...")
        self.run(temp_script=source)

    # 加载脚本
    def load(self, script, load_as_module=False):
        if isinstance(script, str):
            if load_as_module:
                self.load_as_module(script)
            else:
                self._script = script
        else:
            raise ValueError("Script must be type 'str'!")

    # 分析脚本（20171113，20180330改）
    # 原analyse_script()
    def load_from_file(self, file, encoding=None, load_as_module=False):
        script = self.read_from_file(file, encoding=encoding)

        if not load_as_module:
            # 切换到脚本文件所在路径
            try:
                base_name = os.path.basename(file)
                dirname = os.path.abspath(file)
                dirname = os.path.dirname(dirname)
                # os.chdir(dirname)
                # logger.debug(msg='OS change to script''s directory (%s)' % (dirname))
                var_dict = self._registered_context
                var_dict[self.MVAR_SCRIPT_NAME] = os.path.splitext(base_name)[0]
                var_dict[self.MVAR_WORKING_DIR] = dirname
                self._path.clear()
                self.add_to_path(os.path.abspath('.'))  # 最后：程序目录
                self.add_to_path(dirname)  # 倒数第二：脚本目录
                self.init_jinja2_env()
            except:
                pass

        self.load(script, load_as_module=load_as_module)

    # 分析脚本（从字串）（20180403，20180415改）
    def load_from_string(self, source, load_as_module=False):
        script = self.read_from_string(source)
        self.load(script, load_as_module=load_as_module)

    # 2019-9-20：脚本内部异常处理（可被覆写）
    def internal_exception_handler(self, tag=None, args=None, e=None):
        pass

    # 脚本内异常处理
    def handle_exceptions(self, tag, args):
        logger = self._logger
        try:
            self.generate_debug_info(tag, args)

            var_dict = args.var_dict
            handler = var_dict.get(self.MVAR_EXCEPTIONS_HANDLER)
            if handler is not None:
                handler(var_dict)
        except Exception as e:
            logger.error(msg="[PyJsEngine]<handle_exceptions>: {}".format(str(e)))
            logger.debug(msg="------Traceback------\n" + tb.format_exc())

    # 生成调试信息
    def generate_debug_info(self, tag, args):
        var_dict = args.var_dict
        _debug = None
        try:
            _debug = "[{}] TAG: {}({}) | Traceback: {}".format(strftime("%Y-%m-%d %H:%M:%S"), tag.name, repr(tag.attrs),
                                                               str(tb.format_exc()).replace('\n', ' '))
        except:
            _debug = "[{}] TAG: {}({})".format(strftime("%Y-%m-%d %H:%M:%S"), tag.name, repr(tag.attrs))
        var_dict[self.MVAR_DEBUG] = _debug

        # 改在generate_log_datetime()中实现
        # var_dict[self.MVAR_LOG_DATETIME] = strftime("%Y-%m-%d %H:%M:%S")

        return _debug

    # 生成日志时间
    def generate_log_datetime(self, tag, args):
        _log_datetime = strftime("%Y-%m-%d %H:%M:%S")
        var_dict = args.var_dict
        var_dict[self.MVAR_LOG_DATETIME] = _log_datetime

        return _log_datetime

    # 生成层号
    def generate_depth(self, tag, args, depth):
        _depth = depth
        var_dict = args.var_dict
        var_dict[self.MVAR_DEPTH] = _depth

        return _depth

    def attrs_parser(self, args, jskwargs, rules):
        # TODO 待编写（从原attrs_parser移植过来，修改后的需要能兼容原来写的所有attrs_parser()调用）
        #  其他还需处理：engine_get_var()，engine_replace_var()，以及run_*()调用时需要预先处理的工作（改造execute_tag()来实现），等等
        pass

    # TODO 待改造
    # def execute_tag(self, tag, args, depth=0):
    #     logger = self._logger
    #     # 写入日志时间
    #     self.generate_log_datetime(tag, args)
    #     # 写入depth信息到变量
    #     self.generate_depth(tag, args, depth)
    #     # 执行原来的execute_tag()操作，但加入了depth信息
    #     # 2019-3-20：新增优先执行callable对象的标签名，如找不到，则执行runner
    #     callable_names_set = self._callable_names_set
    #     runners = self._tag_runners_map
    #     if isinstance(tag, self.TagItem):
    #         tagn = tag.name
    #         tagn_key = tag.name.split(self.SUB_SPLITTER)[0]
    #         if tagn.startswith(self.REF_KEY_PREFIX):
    #             logger.debug(msg="run_key_call -> {}".format(tagn))  # debug
    #             return self.run_key_call(tag, args, depth)
    #         else:
    #             if tagn in callable_names_set or tagn_key in callable_names_set:
    #                 logger.debug(msg="run_internal_call -> {}".format(tagn))  # debug
    #                 return self.run_internal_call(tag, args, depth)
    #             else:
    #                 runner = runners.get(tagn)
    #                 if runner is not None:
    #                     return runner(tag, args, depth)
    #                 else:
    #                     logger.debug(msg="runner not found! ({})".format(tagn))  # debug
        return None

    # 操作：根节点
    def run_root(self, tag, args, depth=0):
        logger = self._logger
        tagn, taga, tagc = tag.name, tag.attrs, tag.content

        # 属性
        # self.attrs_parser(args, taga, {
        #     self.attrn_abort_on_err: ('abort_on_err', 'b', False)
        # })

        # 处理
        try:
            new_args = deepcopy(args)
            for t in tagc:
                result = self.execute_tag(t, new_args, depth + 1)
                if not result:
                    # 失败即抛错
                    raise RuntimeError("script failed!")
            return True
        except Exception as e:
            # TODO 这类标签内的异常处理，待统一为一个方法调用！（自动捕捉调用的方法名）
            self.internal_exception_handler(tag=tag, args=args, e=e)
            logger.error(msg="[PyJsEngine]<run_root>: {}".format(str(e)))
            logger.debug(msg="------Traceback------\n" + tb.format_exc())
            self.handle_exceptions(tag, args)
            return False

    # 操作：子程序（跟根节点执行的处理一样，其他处理可能不同，所以从root复制过来另外写）
    def run_sub(self, tag, args, depth=0):
        logger = self._logger
        tagn, taga, tagc = tag.name, tag.attrs, tag.content

        # 属性
        self.attrs_parser(args, taga, {
            self.attrn_pass_through: ('pass_through', 'b', False)
        })
        pass_through = args['pass_through']

        # 处理
        try:
            logger.debug(msg="[PyJsEngine]<run_sub>: sub-script entered.")
            new_args = deepcopy(args) if not pass_through else args
            for t in tagc:
                result = self.execute_tag(t, new_args, depth + 1)
                if not result:
                    # 失败则返回上一层程序（正常方式返回）
                    logger.debug(msg="[PyJsEngine]<run_sub>: sub-script exited due to a break.")
                    return True
                    # raise RuntimeError("script failed!")
            logger.debug(msg="[PyJsEngine]<run_sub>: sub-script exited normally.")
            return True
        except Exception as e:
            self.internal_exception_handler(tag=tag, args=args, e=e)
            logger.error(msg="[PyJsEngine]<run_sub>: {}".format(str(e)))
            logger.debug(msg="------Traceback------\n" + tb.format_exc())
            self.handle_exceptions(tag, args)
            return False

    # 操作：循环（利用assert来中断循环，中断时返回True，只有出错才返回False）
    def run_loop(self, tag, args, depth=0):
        logger = self._logger
        tagn, taga, tagc = tag.name, tag.attrs, tag.content

        # 属性
        self.attrs_parser(args, taga, {
            self.attrn_pass_through: ('pass_through', 'b', False)
        })
        pass_through = args['pass_through']

        # 处理
        try:
            logger.debug(msg="[PyJsEngine]<run_loop>: loop started!")
            new_args = deepcopy(args) if not pass_through else args
            while True:
                flag = True
                for t in tagc:
                    result = self.execute_tag(t, new_args, depth + 1)
                    if not result:
                        # 中断则返回上一层程序
                        flag = False
                        logger.debug(msg="[PyJsEngine]<run_loop>: loop break!")
                        break
                if not flag:
                    break
            return True
        except Exception as e:
            self.internal_exception_handler(tag=tag, args=args, e=e)
            logger.error(msg="[PyJsEngine]<run_loop>: {}".format(str(e)))
            logger.debug(msg="------Traceback------\n" + tb.format_exc())
            self.handle_exceptions(tag, args)
            return False

    # 操作：对象内循环（遍历完成或循环中出错时中断循环，中断时返回True，只有出错才返回False）
    def run_for_in(self, tag, args, depth=0):
        logger = self._logger
        tagn, taga, tagc = tag.name, tag.attrs, tag.content

        # 属性
        self.attrs_parser(args, taga, {
            self.attrn_pass_through: ('pass_through', 'b', False),
            self.attrn_key: ('key', 's', None),
            self.attrn_key_eval: ('key_eval', 's', None),
        })
        pass_through = args['pass_through']
        _key = args['key']
        _key_eval = args['key_eval']

        # 处理
        try:
            logger.debug(msg="[PyJsEngine]<run_for_in>: confirming object...")
            var_dict = args.var_dict
            if _key_eval:
                obj = eval(_key_eval)
            elif _key:
                obj = self.engine_get_var(var_dict, _key)
            else:
                raise ValueError("key name illegal!")

            logger.debug(msg="[PyJsEngine]<run_for_in>: loop started!")
            new_args = deepcopy(args) if not pass_through else args
            it = iter(obj)
            while True:
                try:
                    key = next(it)
                    value = obj[key]
                    new_var_dict = new_args.var_dict
                    new_var_dict[self.MVAR_FOR_IN_KEY] = key
                    new_var_dict[self.MVAR_FOR_IN_VALUE] = value

                    flag = True
                    for t in tagc:
                        result = self.execute_tag(t, new_args, depth + 1)
                        if not result:
                            # 中断则返回上一层程序
                            flag = False
                            logger.debug(msg="[PyJsEngine]<run_for_in>: loop break!")
                            break
                    if not flag:
                        break
                except StopIteration:
                    break
            return True
        except Exception as e:
            self.internal_exception_handler(tag=tag, args=args, e=e)
            logger.error(msg="[PyJsEngine]<run_for_in>: {}".format(str(e)))
            logger.debug(msg="------Traceback------\n" + tb.format_exc())
            self.handle_exceptions(tag, args)
            return False

    # 操作：如果/否则如果
    def run_if_elif(self, tag, args, depth=0):
        logger = self._logger
        tagn, taga, tagc = tag.name, tag.attrs, tag.content

        # 属性
        self.attrs_parser(args, taga, {
            self.attrn_pass_through: ('pass_through', 'b', False),
            self.attrn__assert: ('_assert', 'sr', None),
        })
        pass_through = args['pass_through']
        _assert = args['_assert']
        if _assert is not None:
            _assert_tag = self.TagItem()
            _assert_tag.name = _assert
            _assert_tag.attrs = taga
            _assert_tag.content = None
        else:
            _assert_tag = None

        # 处理
        try:
            _condition = _assert_tag
            _elif = None
            _then = None
            _else = None
            for t in tagc:
                if t.name in {self.tagn_assert, self.tagn_assert_not, self.tagn_assert_in, self.tagn_assert_not_in}:
                    _condition = t if _condition is None else _condition
                    # _condition = t
                elif t.name == self.tagn_elif:
                    _elif = t
                elif t.name == self.tagn_then:
                    _then = t
                elif t.name == self.tagn_else:
                    _else = t
            new_args = deepcopy(args) if not pass_through else args
            # 模仿程序语言的if-then-else逻辑，一定要先做_condition，再判断和做其他
            result = self.execute_tag(_condition, new_args, depth + 1)
            if not _then and not _elif and not _else:
                result = False
            else:
                new_args = deepcopy(args) if not pass_through else new_args
                condition_result = result
                result = True
                # 模仿程序语言的if-then-else逻辑，存在_then、_elif或_else时执行，否则为忽略
                if condition_result:
                    if _then:
                        result = self.execute_tag(_then, new_args, depth + 1)
                else:
                    if _elif or _else:
                        _elsth = _elif or _else
                        result = self.execute_tag(_elsth, new_args, depth + 1)
            return result
        except Exception as e:
            self.internal_exception_handler(tag=tag, args=args, e=e)
            logger.error(msg="[PyJsEngine]<run_if_elif>: {}".format(str(e)))
            logger.debug(msg="------Traceback------\n" + tb.format_exc())
            self.handle_exceptions(tag, args)
            return False

    # 操作：那么/否则
    def run_then_else(self, tag, args, depth=0):
        logger = self._logger
        tagn, taga, tagc = tag.name, tag.attrs, tag.content

        # 属性
        # self.attrs_parser(args, taga, {
        #     self.attrn_pass_through: ('pass_through', 'b', False)
        # })
        # pass_through = args['pass_through']

        # 处理
        try:
            # new_args = deepcopy(args)
            new_args = args  # 由<if>控制参数传递，在这里则沿用上层参数
            for t in tagc:
                result = self.execute_tag(t, new_args, depth + 1)
                if not result:
                    # 失败即抛错
                    raise RuntimeError("failed!")
            return True
        except Exception as e:
            self.internal_exception_handler(tag=tag, args=args, e=e)
            logger.error(msg="[PyJsEngine]<run_then_else>: {}".format(str(e)))
            logger.debug(msg="------Traceback------\n" + tb.format_exc())
            self.handle_exceptions(tag, args)
            return False

    # 操作：案例
    def run_case(self, tag, args, depth=0):
        logger = self._logger
        tagn, taga, tagc = tag.name, tag.attrs, tag.content

        # 属性
        self.attrs_parser(args, taga, {
            self.attrn_pass_through: ('pass_through', 'b', False)
        })
        pass_through = args['pass_through']

        # 处理
        try:
            new_args = deepcopy(args) if not pass_through else args
            for t in tagc:
                if t.name in {self.tagn_when, self.tagn_when_others}:
                    result, in_when = self.execute_tag(t, new_args, depth + 1)
                    if not result:
                        # 失败即抛错
                        raise RuntimeError("failed!")
                    if in_when:
                        # 符合情况即打断
                        break
                else:
                    raise RuntimeError("only tags 'when' 'when_others' supported in 'case'!")
            return True
        except Exception as e:
            self.internal_exception_handler(tag=tag, args=args, e=e)
            logger.error(msg="[PyJsEngine]<run_case>: {}".format(str(e)))
            logger.debug(msg="------Traceback------\n" + tb.format_exc())
            self.handle_exceptions(tag, args)
            return False

    # 操作：当……情况（when、when_others）
    def run_when(self, tag, args, depth=0):
        logger = self._logger
        tagn, taga, tagc = tag.name, tag.attrs, tag.content

        # 属性
        self.attrs_parser(args, taga, {
            self.attrn__assert: ('_assert', 'sr', None),
        })
        _assert = args['_assert']
        if _assert is not None:
            _assert_tag = self.TagItem()
            _assert_tag.name = _assert
            _assert_tag.attrs = taga
            _assert_tag.content = None
        else:
            _assert_tag = None

        # 处理
        in_when = False
        try:
            # new_args = deepcopy(args)
            new_args = args  # 由<case>控制参数传递，在这里则沿用上层参数
            if tagn == self.tagn_when:
                _condition = _assert_tag
                if _condition is not None:
                    result = self.execute_tag(_condition, new_args, depth + 1)
                    in_when = True if result else False
                else:
                    _condition = tagc[0]
                    if _condition.name in {self.tagn_assert, self.tagn_assert_not,
                                           self.tagn_assert_in, self.tagn_assert_not_in}:
                        result = self.execute_tag(_condition, new_args, depth + 1)
                        in_when = True if result else False
                    else:
                        raise RuntimeError("tag 'assert*' not found in 'when' at first tag!")
            else:
                in_when = True

            if in_when:
                # 符合when条件，继续执行when下面的tag
                tagc = tagc[1:] if tagn == self.tagn_when else tagc
                for t in tagc:
                    result = self.execute_tag(t, new_args, depth + 1)
                    if not result:
                        # 失败即抛错
                        raise RuntimeError("failed!")
            return True, in_when
        except Exception as e:
            self.internal_exception_handler(tag=tag, args=args, e=e)
            logger.error(msg="[PyJsEngine]<run_when>: {}".format(str(e)))
            logger.debug(msg="------Traceback------\n" + tb.format_exc())
            self.handle_exceptions(tag, args)
            return False, in_when

    # 操作：断言
    def run_assert(self, tag, args, depth=0):
        logger = self._logger
        tagn, taga, tagc = tag.name, tag.attrs, tag.content

        # 属性
        self.attrs_parser(args, taga, {
            self.attrn_key: ('key', 's', None),
            self.attrn_value: ('value', 's', None)
        })
        key = (args['key'] or '').strip()
        value = args['value']

        # 处理
        def my_assert(_value, value):
            # 检查不通过：空值/0/空串/False
            if value is not None:
                if isinstance(_value, int) or isinstance(_value, float):
                    return str(_value) == str(value)
                elif isinstance(_value, bool):
                    return str(_value).lower() == str(value).lower()
                else:
                    return _value == value
            else:
                return not (_value is None or _value == 0 or _value == '' or str(_value).lower() == str(False).lower())

        try:
            var_dict = args.var_dict
            _value = None
            if isinstance(key, str) and key != '':
                # 检查变量值
                _value = self.engine_get_var(var_dict, key)
            elif len(tagc) > 0:
                # 检查内容标签
                # 只要有False即返回False，全检查通过则返回True（and关系）
                for c in tagc:
                    if isinstance(c, self.TagItem):
                        _value = self.execute_tag(c, args, depth + 1)
                        logger.debug("[PyJsEngine]<run_assert>: assert value in sub-tag: {} (with value: {})".format(
                            repr(_value),
                            repr(value)))
                        if not my_assert(_value, value):
                            return False
                return True
            else:
                raise ValueError("assert type illegal!")
            logger.debug(
                "[PyJsEngine]<run_assert>: assert value: {} (with value: {})".format(repr(_value), repr(value)))

            return my_assert(_value, value)
        except Exception as e:
            self.internal_exception_handler(tag=tag, args=args, e=e)
            logger.error(msg="[PyJsEngine]<run_assert>: {}".format(str(e)))
            logger.debug(msg="------Traceback------\n" + tb.format_exc())
            self.handle_exceptions(tag, args)
            return False

    # 操作：否定断言
    def run_assert_not(self, tag, args, depth=0):
        logger = self._logger
        tagn, taga, tagc = tag.name, tag.attrs, tag.content

        # 属性
        self.attrs_parser(args, taga, {
            self.attrn_key: ('key', 's', None),
            self.attrn_value: ('value', 's', None)
        })
        key = (args['key'] or '').strip()
        value = args['value']

        # 处理
        def my_assert_not(_value, value):
            # 检查不通过：非 空值/0/空串/False
            if value is not None:
                if isinstance(_value, int) or isinstance(_value, float):
                    return str(_value) != str(value)
                elif isinstance(_value, bool):
                    return str(_value).lower() != str(value).lower()
                else:
                    return _value != value
            else:
                return _value is None or _value == 0 or _value == '' or str(_value).lower() == str(False).lower()

        try:
            var_dict = args.var_dict
            _value = None
            if isinstance(key, str) and key != '':
                # 检查变量值
                _value = self.engine_get_var(var_dict, key)
            elif len(tagc) > 0:
                # 检查内容标签
                # 只要有True即返回True，全检查不通过则返回False（or关系）
                for c in tagc:
                    if isinstance(c, self.TagItem):
                        _value = self.execute_tag(c, args, depth + 1)
                        logger.debug(
                            "[PyJsEngine]<run_assert_not>: assert value in sub-tag: {} (with value: {})".format(
                                repr(_value),
                                repr(value)))
                        if my_assert_not(_value, value):
                            return True
                return False
            else:
                raise ValueError("assert type illegal!")
            logger.debug(
                "[PyJsEngine]<run_assert_not>: assert value: {} (with value: {})".format(repr(_value), repr(value)))

            return my_assert_not(_value, value)
        except Exception as e:
            self.internal_exception_handler(tag=tag, args=args, e=e)
            logger.error(msg="[PyJsEngine]<run_assert_not>: {}".format(str(e)))
            logger.debug(msg="------Traceback------\n" + tb.format_exc())
            self.handle_exceptions(tag, args)
            return False

    # 操作：断言（在内）
    def run_assert_in(self, tag, args, depth=0):
        logger = self._logger
        tagn, taga, tagc = tag.name, tag.attrs, tag.content

        # 属性
        self.attrs_parser(args, taga, {
            self.attrn_key: ('key', 's', None)
        })
        key = (args['key'] or '').strip()

        # 处理   # TODO testing
        def my_assert(_value, value):
            # 检查不通过：空值/0/空串/False
            if value is not None:
                if isinstance(_value, int) or isinstance(_value, float):
                    return str(_value) == str(value)
                elif isinstance(_value, bool):
                    return str(_value).lower() == str(value).lower()
                else:
                    return _value == value
            else:
                return not (_value is None or _value == 0 or _value == '' or str(_value).lower() == str(False).lower())

        try:
            var_dict = args.var_dict
            _value = None

            if isinstance(key, str) and key != '':
                # 检查变量值
                _value = self.engine_get_var(var_dict, key)
            else:
                raise ValueError("assert key illegal!")

            if len(tagc) > 0:
                # 检查内容标签
                # 只要有False即返回False，全检查通过则返回True（and关系）
                for c in tagc:
                    if isinstance(c, self.TagItem):
                        value = self.execute_tag(c, args, depth + 1)
                        if my_assert(_value, value):
                            return True
                return False
            else:
                raise ValueError("assert value illegal!")
        except Exception as e:
            self.internal_exception_handler(tag=tag, args=args, e=e)
            logger.error(msg="[run_assert_in]<run_assert_in>: {}".format(str(e)))
            logger.debug(msg="------Traceback------\n" + tb.format_exc())
            self.handle_exceptions(tag, args)
            return False

    # 操作：否定断言（在内）
    def run_assert_not_in(self, tag, args, depth=0):
        logger = self._logger
        tagn, taga, tagc = tag.name, tag.attrs, tag.content

        # 属性
        self.attrs_parser(args, taga, {
            self.attrn_key: ('key', 's', None)
        })
        key = (args['key'] or '').strip()

        # 处理   # TODO testing
        def my_assert(_value, value):
            # 检查不通过：空值/0/空串/False
            if value is not None:
                if isinstance(_value, int) or isinstance(_value, float):
                    return str(_value) == str(value)
                elif isinstance(_value, bool):
                    return str(_value).lower() == str(value).lower()
                else:
                    return _value == value
            else:
                return not (_value is None or _value == 0 or _value == '' or str(_value).lower() == str(False).lower())

        try:
            var_dict = args.var_dict
            _value = None

            if isinstance(key, str) and key != '':
                # 检查变量值
                _value = self.engine_get_var(var_dict, key)
            else:
                raise ValueError("assert key illegal!")

            if len(tagc) > 0:
                # 检查内容标签
                # 只要有True即返回True，全检查不通过则返回False（or关系）
                for c in tagc:
                    if isinstance(c, self.TagItem):
                        value = self.execute_tag(c, args, depth + 1)
                        if my_assert(_value, value):
                            return False
                return True
            else:
                raise ValueError("assert value illegal!")
        except Exception as e:
            self.internal_exception_handler(tag=tag, args=args, e=e)
            logger.error(msg="[PyJsEngine]<run_assert_not_in>: {}".format(str(e)))
            logger.debug(msg="------Traceback------\n" + tb.format_exc())
            self.handle_exceptions(tag, args)
            return False

    # 操作：否定
    def run_not(self, tag, args, depth=0):
        logger = self._logger
        tagn, taga, tagc = tag.name, tag.attrs, tag.content

        # 属性
        # self.attrs_parser(args, taga, {
        #     self.attrn_key: ('key', 's', None)
        # })
        # key = (args['key'] or '').strip()

        # 处理
        try:
            value = None
            if len(tagc) > 0 and isinstance(tagc[0], self.TagItem):
                # 检查内容标签（只执行头一个标签）
                value = self.execute_tag(tagc[0], args, depth + 1)
            else:
                raise ValueError("'not' type illegal!")
            logger.debug("[PyJsEngine]<run_not>: 'not' value: {}".format(repr(value)))

            return not value
        except Exception as e:
            self.internal_exception_handler(tag=tag, args=args, e=e)
            logger.error(msg="[PyJsEngine]<run_not>: {}".format(str(e)))
            logger.debug(msg="------Traceback------\n" + tb.format_exc())
            self.handle_exceptions(tag, args)
            return False

    # 操作：逻辑判断
    def run_complex_condition(self, tag, args, depth=0):
        logger = self._logger
        tagn, taga, tagc = tag.name, tag.attrs, tag.content

        # 属性
        self.attrs_parser(args, taga, {
            self.attrn_key1: ('key1', 's', None),
            self.attrn_key2: ('key2', 's', None)
        })
        key1 = (args['key1'] or '').strip()
        key2 = (args['key2'] or '').strip()

        # 处理
        try:
            def get_operator(name):
                if name == self.tagn_and:
                    return 'and'
                elif name == self.tagn_or:
                    return 'or'

            var_dict = args.var_dict
            opr = get_operator(tagn)
            if key1 and key2:
                if isinstance(key1, str) and key1 != '' and isinstance(key2, str) and key2 != '':
                    value1 = self.engine_get_var(var_dict, key1)
                    value2 = self.engine_get_var(var_dict, key2)
                    return eval('value1 ' + opr + ' value2')
                else:
                    raise ValueError("key name illegal!")
            elif len(tagc) > 1:  # 要求2个以上的对象进行操作
                tagc = [c for c in tagc if isinstance(c, self.TagItem)]
                value1 = None
                value2 = None
                result = None
                for nc, c in enumerate(tagc):
                    # 检查内容标签（只执行头一个标签）
                    value2 = self.execute_tag(c, args, depth + 1)
                    if nc == 0:
                        value1 = value2
                    elif nc > 0:
                        result = eval('value1 ' + opr + ' value2')
                        if (opr == 'and' and not result) or (opr == 'or' and result):
                            return result
                        value1 = result
                return value1
            else:
                raise ValueError("complex condition type illegal!")
        except Exception as e:
            self.internal_exception_handler(tag=tag, args=args, e=e)
            logger.error(msg="[PyJsEngine]<run_complex_condition>: {}".format(str(e)))
            logger.debug(msg="------Traceback------\n" + tb.format_exc())
            self.handle_exceptions(tag, args)
            return False

    # 操作：复合条件
    def run_logical_decision(self, tag, args, depth=0):
        logger = self._logger
        tagn, taga, tagc = tag.name, tag.attrs, tag.content

        # 属性
        self.attrs_parser(args, taga, {
            self.attrn_key1: ('key1', 's', None),
            self.attrn_key2: ('key2', 's', None)
        })
        key1 = (args['key1'] or '').strip()
        key2 = (args['key2'] or '').strip()

        # 处理
        try:
            def get_operator(name):
                if name == self.tagn_equal:
                    return '=='
                elif name == self.tagn_not_equal:
                    return '!='
                elif name == self.tagn_less:
                    return '<'
                elif name == self.tagn_less_equal:
                    return '<='
                elif name == self.tagn_greater:
                    return '>'
                elif name == self.tagn_greater_equal:
                    return '>='

            var_dict = args.var_dict
            opr = get_operator(tagn)
            if key1 and key2:
                if isinstance(key1, str) and key1 != '' and isinstance(key2, str) and key2 != '':
                    value1 = self.engine_get_var(var_dict, key1)
                    value2 = self.engine_get_var(var_dict, key2)
                    return eval('value1 ' + opr + ' value2')
                else:
                    raise ValueError("key name illegal!")
            elif len(tagc) > 1:  # 要求2个以上的对象进行操作
                tagc = [c for c in tagc if isinstance(c, self.TagItem)]
                value1 = None
                value2 = None
                for nc, c in enumerate(tagc):
                    # 检查内容标签（只执行头一个标签）
                    value2 = self.execute_tag(c, args, depth + 1)
                    if nc > 0 and not eval('value1 ' + opr + ' value2'):
                        return False
                    value1 = value2
                return True
            else:
                raise ValueError("logical decision type illegal!")
        except Exception as e:
            self.internal_exception_handler(tag=tag, args=args, e=e)
            logger.error(msg="[PyJsEngine]<run_logical_decision>: {}".format(str(e)))
            logger.debug(msg="------Traceback------\n" + tb.format_exc())
            self.handle_exceptions(tag, args)
            return False

    # 操作：预加载模块
    def run_module(self, tag, args, depth=0):
        logger = self._logger
        tagn, taga, tagc = tag.name, tag.attrs, tag.content

        # 属性
        self.attrs_parser(args, taga, {
            self.attrn_src: ('src', 's', None)
        })
        src = args['src']
        try:
            # 特别操作：按顺序检查不加扩展名、加扩展名是否能找到；其中每次按path顺序检查是否能找到
            for ext in self.EXT_NAME_SET:
                esrc = src + ext
                for dir in self._path:
                    nsrc = os.path.join(dir, esrc)
                    if os.path.exists(nsrc):
                        src = nsrc
                        break
            # global_var_dict = self._global_var_dict
            # working_dir = global_var_dict.get(self.MVAR_WORKING_DIR)
            # if working_dir:
            #     src = os.path.join(working_dir, src)
        except:
            pass

        # 处理
        try:
            script_type = self.get_script_type(src)
            logger.debug(msg="[PyJsEngine]<run_module>: src={}, script_type={}".format(repr(src), repr(script_type)))
            self.load_from_file(src, script_type=script_type, load_as_module=True)
            return True
        except Exception as e:
            self.internal_exception_handler(tag=tag, args=args, e=e)
            logger.error(msg="[PyJsEngine]<run_module>: {}".format(str(e)))
            logger.debug(msg="------Traceback------\n" + tb.format_exc())
            self.handle_exceptions(tag, args)
            return False

    # 操作：定义过程
    def run_procedure(self, tag, args, depth=0):
        logger = self._logger
        tagn, taga, tagc = tag.name, tag.attrs, tag.content

        # 属性
        self.attrs_parser(args, taga, {
            self.attrn_name: ('proc_name', 'sr', None)
        })
        proc_name = (args['proc_name'] or '').strip()

        # 处理
        try:
            proc_dict = self._proc_dict
            if isinstance(proc_name, str) and proc_name != '':
                proc_names = proc_name.split(',')
                for proc_name in proc_names:
                    proc_dict[proc_name] = tag
                logger.debug(msg="[PyJsEngine]<run_procedure>: proc_names={} defined.".format(repr(proc_names)))
                return True
            else:
                raise ValueError("proc_name={} name illegal!".format(repr(proc_name)))
        except Exception as e:
            self.internal_exception_handler(tag=tag, args=args, e=e)
            logger.error(msg="[PyJsEngine]<run_procedure>: {}".format(str(e)))
            logger.debug(msg="------Traceback------\n" + tb.format_exc())
            self.handle_exceptions(tag, args)
            return False

    # 操作：调用过程
    def run_call(self, tag, args, depth=0):
        logger = self._logger
        tagn, taga, tagc = tag.name, tag.attrs, tag.content

        # 属性
        self.attrs_parser(args, taga, {
            self.attrn_name: ('proc_name', 's', None),
            self.attrn_pass_through: ('pass_through', 'b', False)
        })
        proc_name = (args['proc_name'] or '').strip()
        pass_through = args['pass_through']

        # 处理
        try:
            proc_dict = self._proc_dict
            if isinstance(proc_name, str) and proc_name != '':
                proc_tag = proc_dict.get(proc_name)
                if proc_tag is not None:
                    logger.debug(msg="[PyJsEngine]<run_call>: proc_name={} found!".format(repr(proc_name)))
                    new_args = deepcopy(args) if not pass_through else args

                    # 2018-10-27新增：把本标签非关键属性的属性作为自定义属性，将属性值导入脚本变量中，这样call传参用起来会更方便！
                    new_var_dict = new_args.var_dict
                    for k, v in taga.items():
                        if k not in {self.attrn_name, self.attrn_pass_through}:
                            v = self.engine_var_replacer(new_var_dict, v) if v is not None else None
                            new_var_dict[k] = v

                    # 进入procedure子标签调用
                    for t in proc_tag.content:
                        result = self.execute_tag(t, new_args, depth + 1)
                        if t.name == self.tagn_return:
                            # 2018-5-4：加入<return>行为（与<get>操作效果一致，但在<call>这里直接return值返回到上层）
                            return result
                        if not result:
                            # 失败则返回上一层程序
                            logger.debug(msg="[PyJsEngine]<run_call>: called procedure exited due to a break.")
                            return False
                            # raise RuntimeError("script failed!")
                    logger.debug(msg="[PyJsEngine]<run_call>: called procedure exited normally.")
                    return True
                else:
                    raise RuntimeError("proc_name={} not found!".format(repr(proc_name)))
            else:
                raise ValueError("proc_name={} name illegal!".format(repr(proc_name)))
        except Exception as e:
            self.internal_exception_handler(tag=tag, args=args, e=e)
            logger.error(msg="[PyJsEngine]<run_call>: {}".format(str(e)))
            logger.debug(msg="------Traceback------\n" + tb.format_exc())
            self.handle_exceptions(tag, args)
            return False

    # 操作：调用过程（根据变量条件）
    def run_cond_call(self, tag, args, depth=0):
        logger = self._logger
        tagn, taga, tagc = tag.name, tag.attrs, tag.content

        # 属性
        self.attrs_parser(args, taga, {
            self.attrn_name: ('proc_name', 's', None),
            # self.attrn_pass_through: ('pass_through', 'b', False)
        })
        proc_name = (args['proc_name'] or '').strip()
        # pass_through = args['pass_through']

        # 处理
        try:
            # proc_dict = self._proc_dict
            if isinstance(proc_name, str) and proc_name != '':
                var_dict = args.var_dict
                proc_privilege_set = var_dict.get(self.MVAR_PROC_PRIVILEGE) if isinstance(
                    var_dict.get(self.MVAR_PROC_PRIVILEGE),
                    set) else set()
                proc_switcher = var_dict.get(self.PROC_SWITCHER_PREFIX + proc_name)
                proc_switcher = str(proc_switcher) if proc_switcher is not None else None
                if proc_switcher == self.PROC_SWITCHER_FLAG and proc_name in proc_privilege_set:
                    # 直接调用原run_call()方法
                    self.run_call(tag, args, depth=depth)
            else:
                raise ValueError("proc_name={} name illegal!".format(repr(proc_name)))
        except Exception as e:
            self.internal_exception_handler(tag=tag, args=args, e=e)
            logger.error(msg="[PyJsEngine]<run_cond_call>: {}".format(str(e)))
            logger.debug(msg="------Traceback------\n" + tb.format_exc())
            self.handle_exceptions(tag, args)
            return False

    # 操作：渲染模板
    def run_template(self, tag, args, depth=0):
        logger = self._logger
        tagn, taga, tagc = tag.name, tag.attrs, tag.content

        # 属性
        self.attrs_parser(args, taga, {
            self.attrn_key: ('key', 's', None),
            self.attrn__from_file: ('_from_file', 's', None),
            self.attrn__to_file: ('_to_file', 's', None),
            self.attrn__to_key: ('_to_key', 's', None),
            self.attrn_content: ('content', 's', None),
            self.attrn_encoding: ('encoding', 's', None),
        })
        key = (args['key'] or '').strip()
        _from_file = args['_from_file']
        _to_file = args['_to_file']
        _to_key = args['_to_key']
        content = args['content']
        encoding = args['encoding'] or self._encoding

        # 处理
        try:
            var_dict = args.var_dict
            fp = None
            template_str = None
            template = None
            if isinstance(key, str) and key != '':
                template_str = self.engine_get_var(var_dict, key)
            elif isinstance(content, str):
                template_str = content
            elif _from_file is not None:
                # 支持引用io变量，故在此不限制它是str类型！
                from_real_file = False
                if isinstance(_from_file, str):
                    # 先尝试从jinja2 env加载文件
                    env = self._j2_env
                    try:
                        template = env.get_template(_from_file)
                        logger.debug("[PyJsEngine]<run_template>: Template loaded with env.")
                    except Exception as e:
                        template = None
                        logger.debug("[PyJsEngine]<run_template>: Loading template with env failed! ({})".format(
                            str(e)))
                    if template is None:
                        fp = open(_from_file, "r", encoding=encoding)
                        from_real_file = True
                else:
                    fp = _from_file

            if template is None:
                try:
                    template_str = fp.read()
                finally:
                    if from_real_file:
                        fp.close()
                logger.debug("[PyJsEngine]<run_template>: template_str={}".format(repr(template_str)))
                template = Template(template_str)

            global_var_dict = self._global_var_dict
            chain_var_dict = ChainMap(var_dict, global_var_dict)
            template_render_result = template.render(**chain_var_dict)
            # logger.debug("[PyJsEngine]<run_template>: template_render_result={}".format(repr(template_render_result)))
            logger.debug("[PyJsEngine]<run_template>: len(template_render_result)={}".format(
                len(template_render_result) if template_render_result is not None else -1))

            # 输出
            if isinstance(_to_key, str) and _to_key != '':
                var_dict[_to_key] = template_render_result
                logger.debug("[PyJsEngine]<run_template>: save to key {}".format(repr(_to_key)))
                return True
            elif _to_file is not None:
                # 支持引用io变量，故在此不限制它是str类型！
                to_real_file = False
                if isinstance(_to_file, str):
                    fp = open(_to_file, "w", encoding=encoding)
                    to_real_file = True
                else:
                    fp = _to_file
                try:
                    fp.write(template_render_result)
                finally:
                    if to_real_file:
                        fp.close()
                return True
            return template_render_result
        except Exception as e:
            self.internal_exception_handler(tag=tag, args=args, e=e)
            logger.error(msg="[PyJsEngine]<run_template>: {}".format(str(e)))
            logger.debug(msg="------Traceback------\n" + tb.format_exc())
            self.handle_exceptions(tag, args)
            return False

    # 操作：json_dump json对象转字串
    def run_json_dump(self, tag, args, depth=0):
        logger = self._logger
        tagn, taga, tagc = tag.name, tag.attrs, tag.content

        # 属性
        self.attrs_parser(args, taga, {
            self.attrn_key: ('key', 's', None),
            # self.attrn__from_file: ('_from_file', 's', None),
            self.attrn__to_file: ('_to_file', 's', None),
            self.attrn__to_key: ('_to_key', 's', None),
            # self.attrn_content: ('content', 's', None),
            self.attrn_encoding: ('encoding', 's', None),
        })
        key = (args['key'] or '').strip()
        # _from_file = args['_from_file']
        _to_file = args['_to_file']
        _to_key = args['_to_key']
        # content = args['content']
        encoding = args['encoding'] or self._encoding

        # 处理  # TODO testing
        try:
            var_dict = args.var_dict
            json_obj = None
            if isinstance(key, str) and key != '':
                json_obj = self.engine_get_var(var_dict, key)

            json_str = json.dumps(json_obj)
            logger.debug("[PyJsEngine]<run_json_dump>: json_str={}".format(repr(json_str)))

            # 输出
            if isinstance(_to_key, str) and _to_key != '':
                var_dict[_to_key] = json_str
                logger.debug("[PyJsEngine]<run_json_dump>: save to key {}".format(repr(_to_key)))
                return True
            elif _to_file is not None:
                # 支持引用io变量，故在此不限制它是str类型！
                to_real_file = False
                if isinstance(_to_file, str):
                    fp = open(_to_file, "w", encoding=encoding)
                    to_real_file = True
                else:
                    fp = _to_file
                try:
                    fp.write(json_str)
                finally:
                    if to_real_file:
                        fp.close()
                return True
            return json_str
        except Exception as e:
            self.internal_exception_handler(tag=tag, args=args, e=e)
            logger.error(msg="[PyJsEngine]<run_json_dump>: {}".format(str(e)))
            logger.debug(msg="------Traceback------\n" + tb.format_exc())
            self.handle_exceptions(tag, args)
            return False

    # 操作：json_load json字串转对象
    def run_json_load(self, tag, args, depth=0):
        logger = self._logger
        tagn, taga, tagc = tag.name, tag.attrs, tag.content

        # 属性
        self.attrs_parser(args, taga, {
            self.attrn_key: ('key', 's', None),
            self.attrn__from_file: ('_from_file', 's', None),
            # self.attrn__to_file: ('_to_file', 's', None),
            self.attrn__to_key: ('_to_key', 's', None),
            self.attrn_content: ('content', 's', None),
            self.attrn_encoding: ('encoding', 's', None),
        })
        key = (args['key'] or '').strip()
        _from_file = args['_from_file']
        # _to_file = args['_to_file']
        _to_key = args['_to_key']
        content = args['content']
        encoding = args['encoding'] or self._encoding

        # 处理  # TODO testing
        try:
            var_dict = args.var_dict
            json_str = None
            if isinstance(key, str) and key != '':
                json_str = self.engine_get_var(var_dict, key)
            elif isinstance(content, str):
                json_str = content
            elif _from_file is not None:
                # 支持引用io变量，故在此不限制它是str类型！
                from_real_file = False
                if isinstance(_from_file, str):
                    fp = open(_from_file, "r", encoding=encoding)
                    from_real_file = True
                else:
                    fp = _from_file
                try:
                    json_str = fp.read()
                finally:
                    if from_real_file:
                        fp.close()

            json_obj = json.loads(json_str)
            logger.debug("[PyJsEngine]<run_json_load>: json_obj={}".format(repr(json_obj)))

            # 输出
            if isinstance(_to_key, str) and _to_key != '':
                var_dict[_to_key] = json_obj
                logger.debug("[PyJsEngine]<run_json_load>: save to key {}".format(repr(_to_key)))
                return True
            return json_obj
        except Exception as e:
            self.internal_exception_handler(tag=tag, args=args, e=e)
            logger.error(msg="[PyJsEngine]<run_json_load>: {}".format(str(e)))
            logger.debug(msg="------Traceback------\n" + tb.format_exc())
            self.handle_exceptions(tag, args)
            return False

    # 操作：定义变量
    def run_set(self, tag, args, depth=0):
        logger = self._logger
        tagn, taga, tagc = tag.name, tag.attrs, tag.content

        # 属性
        self.attrs_parser(args, taga, {
            self.attrn_type: ('set_type', 's', self.SET_TYPE_VALUE),
            self.attrn_key: ('key', 's', None),
            self.attrn_value: ('value', 's', None),
            self.attrn_global: ('global', 'b', False)
        })
        set_type = args['set_type']
        key = (args['key'] or '').strip()
        value = args['value'] or None
        global_ = args['global']

        # 处理
        try:
            global_var_dict = self._global_var_dict
            local_var_dict = args.var_dict
            if isinstance(key, str) and key != '':
                logger.debug(
                    "[PyJsEngine]<run_set>: setting {}({})...".format(repr(key), repr(set_type)))
                if set_type == self.SET_TYPE_OBJECT:
                    if len(tagc) > 0 and isinstance(tagc[0], self.TagItem):
                        value = self.execute_tag(tagc[0], args, depth + 1)
                        if global_:
                            global_var_dict[key] = value
                        else:
                            local_var_dict[key] = value
                    else:
                        raise ValueError("tag content illegal!")
                elif set_type == self.SET_TYPE_EVAL:
                    value = eval(value)
                    if global_:
                        global_var_dict[key] = value
                    else:
                        local_var_dict[key] = value
                elif set_type == self.SET_TYPE_VALUE:
                    if global_:
                        global_var_dict[key] = value
                    else:
                        local_var_dict[key] = value
                else:
                    raise ValueError("set_type illegal!")
                logger.debug("[PyJsEngine]<run_set>: value={}, type(value)={}".format(repr(value), repr(type(value))))
                return True
            else:
                raise ValueError("key name illegal!")
        except Exception as e:
            self.internal_exception_handler(tag=tag, args=args, e=e)
            logger.error(msg="[PyJsEngine]<run_set>: {}".format(str(e)))
            logger.debug(msg="------Traceback------\n" + tb.format_exc())
            self.handle_exceptions(tag, args)
            return False

    # 操作：定义变量（带类型）
    def run_set_with_type(self, tag, args, depth=0):
        logger = self._logger
        tagn, taga, tagc = tag.name, tag.attrs, tag.content

        # 属性
        # pass

        # 处理
        try:
            var_dict = args.var_dict
            tmp_dict = {}
            for k, v in taga.items():
                if tagn in {self.tagn_set_int, self.tagn_set_num, self.tagn_set_str, }:
                    v = self.engine_var_replacer(var_dict, v) if v is not None else None
                    if tagn == self.tagn_set_int:
                        try:
                            v = int(v)
                        except ValueError:
                            v = int(float(v))
                    elif tagn == self.tagn_set_num:
                        v = float(v)
                    elif tagn == self.tagn_set_str:
                        v = str(v)
                elif tagn in {self.tagn_set_eval}:
                    v = eval(v)
                else:
                    raise RuntimeError()
                tmp_dict[k] = v
                logger.debug("[PyJsEngine]<run_set_with_type>: preparing {} = {} ...".format(repr(k), repr(v)))
            var_dict.update(tmp_dict)  # 检查到全部变量定义不存在问题，才执行update进行更新

            return True
        except Exception as e:
            self.internal_exception_handler(tag=tag, args=args, e=e)
            logger.error(msg="[PyJsEngine]<run_set_with_type>: {}".format(str(e)))
            logger.debug(msg="------Traceback------\n" + tb.format_exc())
            self.handle_exceptions(tag, args)
            return False

    # 操作：取得变量
    def run_get(self, tag, args, depth=0):
        logger = self._logger
        tagn, taga, tagc = tag.name, tag.attrs, tag.content

        # 属性
        self.attrs_parser(args, taga, {
            self.attrn_key: ('key', 's', None),
            self.attrn_default_value: ('defvalue', 's', None),
            self.attrn_index: ('index', 's', None),
            self.attrn_dict_key: ('dict_key', 's', None)
        })
        key = (args['key'] or '').strip()
        defvalue = args['defvalue']
        index = args['index']
        dict_key = args['dict_key']

        # 处理
        try:
            var_dict = args.var_dict
            if isinstance(key, str) and key != '':
                logger.debug(
                    "[PyJsEngine]<run_get>: getting {}...".format(repr(key)))
                value = self.engine_get_var(var_dict, key, default=defvalue)
                logger.debug("[PyJsEngine]<run_get>: value={}, type(value)={}".format(repr(value), repr(type(value))))
                if dict_key:
                    return value.get(dict_key, defvalue)
                elif index:
                    return value[int(index)] or defvalue
                else:
                    return value
            elif defvalue is not None:
                return defvalue
            else:
                raise ValueError("key name illegal!")
        except Exception as e:
            self.internal_exception_handler(tag=tag, args=args, e=e)
            logger.error(msg="[PyJsEngine]<run_get>: {}".format(str(e)))
            logger.debug(msg="------Traceback------\n" + tb.format_exc())
            self.handle_exceptions(tag, args)
            return False

    # 统计文本文件行数
    @staticmethod
    def count_file_lines(file_name, encoding=None):
        count = -1
        if encoding:
            with open(file_name, mode='rU', encoding=encoding) as fp:
                for count, line in enumerate(fp):
                    pass
        else:
            with open(file_name, mode='rU') as fp:
                for count, line in enumerate(fp):
                    pass
        count += 1
        return count

    # 操作：加载变量清单
    def run_load_vars(self, tag, args, depth=0):
        logger = self._logger
        tagn, taga, tagc = tag.name, tag.attrs, tag.content

        # 属性
        self.attrs_parser(args, taga, {
            self.attrn_type: ('file_type', 's', None),
            self.attrn_name: ('file_name', 's', None),
            self.attrn_encoding: ('encoding', 's', None),
            # self.attrn_pass_through: ('pass_through', 'b', False),
            self.attrn_auto_strip: ('auto_strip', 'b', True),
            self.attrn_allow_none: ('allow_none', 'b', False)
        })
        file_type = args['file_type'] or self.FILE_TYPE_CSV
        file_name = (args['file_name'] or '')  # .strip()
        encoding = args['encoding'] or self._encoding
        # pass_through = args['pass_through']
        auto_strip = args['auto_strip']
        allow_none = args['allow_none']
        try:
            for dir in self._path:
                n_file_name = os.path.join(dir, file_name)
                if os.path.exists(n_file_name):
                    file_name = n_file_name
                    break

            # global_var_dict = self._global_var_dict
            # working_dir = global_var_dict.get(self.MVAR_WORKING_DIR)
            # if working_dir:
            #     file_name = os.path.join(working_dir, file_name)
        except:
            pass

        # 处理
        try:
            logger.debug(msg="[PyJsEngine]<run_load_vars>: loading vars started! ({})".format(file_name))
            if isinstance(file_name, str) and file_name != '':
                if file_type == self.FILE_TYPE_CSV:
                    fp = None
                    try:
                        if encoding:
                            fp = open(file_name, mode='r', newline='', encoding=encoding)
                        else:
                            fp = open(file_name, mode='r', newline='')
                        cr = csv.reader(fp)
                        for nr, r in enumerate(cr):
                            if len(r) <= 0 or len([1 for i in r if i is not None]) <= 0:
                                # 跳过完全空行
                                logger.info(
                                    msg="[PyJsEngine]<run_load_vars>: skipping empty row... ({})".format(
                                        str(nr + 1)))
                                continue

                            # 处理变量名
                            var_name = r[0]
                            if not var_name:
                                logger.info(
                                    msg="[PyJsEngine]<run_load_vars>: skipping empty var name... ({})".format(
                                        str(nr + 1)))
                                continue
                            var_name = var_name.strip()
                            # 处理变量值
                            var_value = r[1]
                            if auto_strip or allow_none:
                                if not allow_none and var_value is None:
                                    var_value = ''
                                if auto_strip and isinstance(var_value, str):
                                    var_value = var_value.strip()

                            # 存入变量字典
                            args.var_dict[var_name] = var_value
                            logger.info(
                                msg="[PyJsEngine]<run_load_vars>: var stored! ({} -> {}) ({})".format(
                                    repr(var_name), repr(var_value), str(nr + 1)))
                    except Exception as e:
                        raise e
                    finally:
                        # 关闭文件
                        if fp:
                            fp.close()
                else:
                    raise ValueError("file type illegal!")
            else:
                raise ValueError("file name illegal!")
            logger.debug(msg="[PyJsEngine]<run_load_vars>: loading vars finished! ({})".format(file_name))
            return True
        except Exception as e:
            self.internal_exception_handler(tag=tag, args=args, e=e)
            logger.error(msg="[PyJsEngine]<run_load_vars>: {}".format(str(e)))
            logger.debug(msg="------Traceback------\n" + tb.format_exc())
            self.handle_exceptions(tag, args)
            return False

    def load_data_file(self, file_name, file_type=FILE_TYPE_CSV, encoding=None, **kwargs):
        logger = self._logger
        encoding = encoding or self._encoding
        data_list = []
        fieldnames = []

        if file_type == self.FILE_TYPE_CSV:
            auto_strip = kwargs.get('auto_strip')
            allow_none = kwargs.get('allow_none')

            count = max(self.count_file_lines(file_name, encoding=encoding) - 1, 0)
            logger.info(msg="[PyJsEngine]<load_data_file>: lines count: {}".format(str(count)))

            fp = None
            try:
                if encoding:
                    fp = open(file_name, mode='r', newline='', encoding=encoding)
                else:
                    fp = open(file_name, mode='r', newline='')
                cdr = csv.DictReader(fp)
                fieldnames = cdr.fieldnames
                fieldnames = [k.replace(',', '_') for k in fieldnames]  # 将字段名中的英文逗号替换为“_”
                for nd, d in enumerate(cdr):
                    flag = True
                    if len(d) <= 0 or len([1 for a, b in d.items() if b is not None]) <= 0:
                        # 跳过完全空行
                        logger.info(
                            msg="[PyJsEngine]<load_data_file>: skipping empty row... ({}/{})".format(
                                str(nd + 1),
                                str(count)))
                        continue
                    d = {k.replace(',', '_'): v for k, v in d.items()}  # 将字段名中的英文逗号替换为“_”

                    new_d = d
                    if auto_strip or allow_none:
                        # 2018-5-4：新增开关 auto_strip自动裁剪 allow_none允许空值（不转换为空串）
                        new_d = {}
                        for k, v in d.items():
                            new_k = k
                            new_v = v
                            if not allow_none and new_v is None:
                                new_v = ''
                            if auto_strip and isinstance(new_v, str):
                                new_v = new_v.strip()
                            new_d[new_k] = new_v

                    # 2019-9-17：排除MVAR字段名，避免MVAR被覆盖（包括权限设置等）
                    new_d = {k: v for k, v in new_d.items() if k not in self.MVAR_SET}

                    data_list.append(new_d)
            except Exception as e:
                raise e
            finally:
                # 关闭文件
                if fp:
                    fp.close()
        else:
            raise ValueError("file type illegal!")

        return data_list, fieldnames, count

    # 操作：加载数据
    def run_load_data(self, tag, args, depth=0):
        logger = self._logger
        tagn, taga, tagc = tag.name, tag.attrs, tag.content

        # 属性
        self.attrs_parser(args, taga, {
            self.attrn_type: ('file_type', 's', None),
            self.attrn_name: ('file_name', 's', None),
            self.attrn_encoding: ('encoding', 's', None),
            self.attrn_pass_through: ('pass_through', 'b', False),
            self.attrn_auto_strip: ('auto_strip', 'b', True),
            self.attrn_allow_none: ('allow_none', 'b', False)
        })
        file_type = args['file_type'] or self.FILE_TYPE_CSV
        file_name = (args['file_name'] or '')  # .strip()
        encoding = args['encoding'] or self._encoding
        pass_through = args['pass_through']
        auto_strip = args['auto_strip']
        allow_none = args['allow_none']
        try:
            for dir in self._path:
                n_file_name = os.path.join(dir, file_name)
                if os.path.exists(n_file_name):
                    file_name = n_file_name
                    break

            # global_var_dict = self._global_var_dict
            # working_dir = global_var_dict.get(self.MVAR_WORKING_DIR)
            # if working_dir:
            #     file_name = os.path.join(working_dir, file_name)
        except:
            pass

        # 处理
        try:
            logger.debug(msg="[PyJsEngine]<run_load_data>: loading data started! ({})".format(file_name))
            if isinstance(file_name, str) and file_name != '':
                data_list, fieldnames, count = self.load_data_file(file_name, file_type=file_type, encoding=encoding,
                                                                   auto_strip=auto_strip, allow_none=allow_none)
                logger.info(msg="[PyJsEngine]<run_load_data>: lines count: {}".format(str(count)))
                logger.info(msg="[PyJsEngine]<run_load_data>: fieldnames: {}".format(repr(fieldnames)))
                args.var_dict[self.MVAR_LOADED_DATA_COUNT] = count

                flag = True
                for nd, d in enumerate(data_list):
                    # TODO testing

                    new_args = deepcopy(args) if not pass_through else args
                    new_args.var_dict.update(d)

                    # 2019-9-17：只允许更大的索引值覆盖先前保存的小的索引值（除非置为None）
                    # index改为从1计起，适应实际需求
                    cur_ind = new_args.var_dict.get(self.MVAR_LOADED_DATA_ITEM_INDEX)
                    new_ind = nd + 1
                    if cur_ind is None or type(cur_ind) != type(new_ind) or cur_ind < new_ind:
                        new_args.var_dict[self.MVAR_LOADED_DATA_ITEM_INDEX] = new_ind

                    new_args.var_dict[self.MVAR_LOADED_DATA_COLS] = ",".join(fieldnames)
                    logger.info(
                        msg="[PyJsEngine]<run_load_data>: row read! ({}/{})".format(str(nd + 1), str(count)))
                    result = None
                    for t in tagc:
                        result = self.execute_tag(t, new_args, depth + 1)
                        if not result:
                            # 中断则返回上一层程序
                            flag = False
                            logger.debug(msg="[PyJsEngine]<run_load_data>: loop break!")
                            break
                    if not flag:
                        break

                # args.var_dict.pop(self.MVAR_LOADED_DATA_COUNT)  # 改在finally中处理
            else:
                raise ValueError("file name illegal!")
            logger.debug(msg="[PyJsEngine]<run_load_data>: loading data finished! ({})".format(file_name))
            return True
        except Exception as e:
            self.internal_exception_handler(tag=tag, args=args, e=e)
            logger.error(msg="[PyJsEngine]<run_load_data>: {}".format(str(e)))
            logger.debug(msg="------Traceback------\n" + tb.format_exc())
            self.handle_exceptions(tag, args)
            return False
        finally:
            if self.MVAR_LOADED_DATA_COUNT in args.var_dict:
                args.var_dict.pop(self.MVAR_LOADED_DATA_COUNT)

    # 输出数据到文件
    @staticmethod
    def write_data_to_file(file_name, data, binary_data=False, newline=None, encoding=None):
        fp = None
        try:
            if binary_data:
                fp = open(file_name, 'wb')
            else:
                if encoding:
                    fp = open(file_name, 'w', newline=newline, encoding=encoding)
                else:
                    fp = open(file_name, 'w', newline=newline)
            fp.write(data)
        finally:
            if fp:
                fp.close()

    # 2019-9-21：获取基于基本文件名的output锁
    def get_output_lock(self, file_name):
        with self._output_lmap_lock:
            base_name = os.path.basename(file_name)
            lock = self._output_lmap.setdefault(base_name, Lock())
            return lock

    # 操作：存储数据
    def run_output(self, tag, args, depth=0):
        logger = self._logger
        tagn, taga, tagc = tag.name, tag.attrs, tag.content

        # 属性
        self.attrs_parser(args, taga, {
            self.attrn_type: ('file_type', 's', None),
            self.attrn_name: ('file_name', 's', None),
            self.attrn_encoding: ('encoding', 's', None),
            self.attrn_newline: ('newline', 's', None),
            self.attrn_mode: ('mode', 's', None),
            self.attrn_cols: ('cols', 's', None)
        })
        file_type = args['file_type'] or self.FILE_TYPE_CSV
        file_name = (args['file_name'] or '')  # .strip()
        encoding = args['encoding'] or self._encoding
        newline = args['newline']
        mode = args['mode'] or 'a'  # output方式默认为append
        cols = (args['cols'] or '').strip()
        cols = [c.strip() for c in cols.split(',')]
        try:
            n_file_name = os.path.join(self._path[0], file_name)
            file_name = n_file_name

            # for dir in self._path:
            #     n_file_name = os.path.join(dir, file_name)
            #     if os.path.exists(n_file_name):
            #         file_name = n_file_name
            #         break

            # global_var_dict = self._global_var_dict
            # working_dir = global_var_dict.get(self.MVAR_WORKING_DIR)
            # if working_dir:
            #     file_name = os.path.join(working_dir, file_name)
        except:
            pass

        # 处理
        try:
            logger.debug(msg="[PyJsEngine]<run_output>: output started!")
            if isinstance(file_name, str) and file_name != '':
                with self.get_output_lock(file_name):
                    if len(tagc) > 0 and isinstance(tagc[0], self.TagItem):
                        # 子标签第1个标签返回的结果作为文件数据写入到文件（无视输出文件类型）
                        data = self.execute_tag(tagc[0], args, depth + 1)
                        try:
                            self.write_data_to_file(file_name=file_name, data=data,
                                                    binary_data=isinstance(data, bytes),
                                                    newline=newline,
                                                    encoding=encoding)
                        except Exception as e:
                            raise e
                    else:
                        if file_type == self.FILE_TYPE_CSV:
                            if len(cols) > 0:
                                fp = None
                                try:
                                    if encoding:
                                        fp = open(file_name, mode=mode, newline='', encoding=encoding)
                                    else:
                                        fp = open(file_name, mode=mode, newline='')
                                    cdw = csv.DictWriter(fp, cols)
                                    # if mode.find('w') >= 0:
                                    if fp.tell() <= 0:
                                        # 新建或覆盖时才写入标题字段
                                        cdw.writeheader()

                                    # 写入数据
                                    var_dict = args.var_dict
                                    row = {}
                                    for c in cols:
                                        # 改成取连同全局变量在内的变量
                                        row[c] = self.engine_get_var(var_dict, c)
                                        # row[c] = var_dict.get(c, None)
                                    cdw.writerow(row)
                                    logger.info(msg="[PyJsEngine]<run_output>: row written!")
                                except Exception as e:
                                    raise e
                                finally:
                                    # 关闭文件
                                    if fp:
                                        fp.close()
                            else:
                                raise ValueError("columns illegal!")
                        else:
                            raise ValueError("file type illegal!")
            else:
                raise ValueError("file name illegal!")
            logger.debug(msg="[PyJsEngine]<run_output>: output finished!")
            return True
        except Exception as e:
            self.internal_exception_handler(tag=tag, args=args, e=e)
            logger.error(msg="[PyJsEngine]<run_output>: {}".format(str(e)))
            logger.debug(msg="------Traceback------\n" + tb.format_exc())
            self.handle_exceptions(tag, args)
            return False

    # 操作：迭代生成
    def run_iter_make(self, tag, args, depth=0):
        logger = self._logger
        tagn, taga, tagc = tag.name, tag.attrs, tag.content

        # 属性
        self.attrs_parser(args, taga, {
            self.attrn_key: ('key', 's', None),
            self.attrn_src: ('src_key', 's', None)
        })
        key = (args['key'] or '').strip()
        src_key = (args['src_key'] or '').strip()

        # 处理   # TODO testing
        try:
            var_dict = args.var_dict
            if isinstance(key, str) and key != '':
                if isinstance(src_key, str) and src_key != '':
                    value = self.engine_get_var(var_dict, src_key)
                    result = None
                    try:
                        result = iter(value)
                    except:
                        raise ValueError("source value not iterable!")
                    var_dict[key] = result
                    logger.debug("[PyJsEngine]<run_iter_make>: type(result)={}".format(repr(type(result))))
                    return True
                else:
                    raise ValueError("source key name illegal!")
            else:
                raise ValueError("key name illegal!")
        except Exception as e:
            self.internal_exception_handler(tag=tag, args=args, e=e)
            logger.error(msg="[PyJsEngine]<run_iter_make>: {}".format(str(e)))
            logger.debug(msg="------Traceback------\n" + tb.format_exc())
            self.handle_exceptions(tag, args)
            return False

    # 操作：迭代下项
    def run_iter_next(self, tag, args, depth=0):
        logger = self._logger
        tagn, taga, tagc = tag.name, tag.attrs, tag.content

        # 属性
        self.attrs_parser(args, taga, {
            self.attrn_key: ('key', 's', None)
        })
        key = (args['key'] or '').strip()

        # 处理   # TODO testing
        try:
            var_dict = args.var_dict
            if isinstance(key, str) and key != '':
                value = self.engine_get_var(var_dict, key)
                result = None
                try:
                    result = next(value)
                except StopIteration:
                    return True
                except:
                    raise ValueError("value not iterable!")
                logger.debug("[PyJsEngine]<run_iter_next>: type(result)={}".format(repr(type(result))))
                return result
            else:
                raise ValueError("key name illegal!")
        except Exception as e:
            self.internal_exception_handler(tag=tag, args=args, e=e)
            logger.error(msg="[PyJsEngine]<run_iter_next>: {}".format(str(e)))
            logger.debug(msg="------Traceback------\n" + tb.format_exc())
            self.handle_exceptions(tag, args)
            return False

    # 操作：定义异常处理
    def run_handle_exceptions(self, tag, args, depth=0):
        logger = self._logger
        tagn, taga, tagc = tag.name, tag.attrs, tag.content

        # 属性
        self.attrs_parser(args, taga, {
            self.attrn_type: ('file_type', 's', None),
            self.attrn_name: ('file_name', 's', None),
            self.attrn_cols: ('cols', 's', None),
            self.attrn_debug: ('debug', 'b', False)
        })
        file_type = args['file_type'] or self.FILE_TYPE_CSV
        file_name = (args['file_name'] or '')  # .strip()
        cols = (args['cols'] or '').strip()
        cols = [c.strip() for c in cols.split(',')]
        debug = args['debug']
        try:
            n_file_name = os.path.join(self._path[0], file_name)
            file_name = n_file_name

            # for dir in self._path:
            #     n_file_name = os.path.join(dir, file_name)
            #     if os.path.exists(n_file_name):
            #         file_name = n_file_name
            #         break

            # global_var_dict = self._global_var_dict
            # working_dir = global_var_dict.get(self.MVAR_WORKING_DIR)
            # if working_dir:
            #     file_name = os.path.join(working_dir, file_name)
        except:
            pass

        # 处理
        try:
            var_dict = args.var_dict
            if isinstance(file_name, str) and file_name != '':
                if file_type == self.FILE_TYPE_CSV:
                    if debug:
                        cols.append(self.MVAR_DEBUG)
                    if len(cols) > 0:
                        def handler(vars):
                            is_exists = os.path.exists(file_name)
                            fp = open(file_name, mode='a', newline='')
                            cdw = csv.DictWriter(fp, cols)
                            if not is_exists:
                                cdw.writeheader()
                            row = {}
                            for c in cols:
                                row[c] = vars.get(c, None)
                            cdw.writerow(row)
                            fp.close()

                        var_dict[self.MVAR_EXCEPTIONS_HANDLER] = handler
                        logger.debug(msg="[PyJsEngine]<run_handle_exceptions>: exceptions handler defined!")
                    else:
                        raise ValueError("columns illegal!")
                else:
                    raise ValueError("file type illegal!")
            else:
                raise ValueError("file name illegal!")
            return True
        except Exception as e:
            self.internal_exception_handler(tag=tag, args=args, e=e)
            logger.error(msg="[PyJsEngine]<run_handle_exceptions>: {}".format(str(e)))
            logger.debug(msg="------Traceback------\n" + tb.format_exc())
            self.handle_exceptions(tag, args)
            return False

    def subprocess_popen(self, *args, cwd=None):
        si = subprocess.STARTUPINFO()
        si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        # 注：shell开关影响执行命令行shell命令，开启会影响pyinstaller编译后的程序不能正常调用其他程序！
        p = subprocess.Popen(*args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             # shell=True,
                             startupinfo=si, cwd=cwd)
        msgs = [line.decode(self.SHELL_ENCODING_DEFAULT) for line in p.stdout.readlines()]
        status = p.wait()
        return status, msgs

    # 操作：执行os命令 call_os_cmd
    def run_call_os_cmd(self, tag, args, depth=0):
        logger = self._logger
        tagn, taga, tagc = tag.name, tag.attrs, tag.content

        # 属性
        self.attrs_parser(args, taga, {
            self.attrn_cmd: ('os_cmd', 's', ''),
            self.attrn_args: ('os_args', 's', ''),
        })
        os_cmd = args['os_cmd']
        os_args = args['os_args']

        # 处理
        try:
            status = None
            if os_cmd != '':
                logger.info(msg="[PyJsEngine]<call_os_cmd>: Executing OS command... (<%s><%s>)" % (
                    repr(os_cmd), repr(os_args)))
                logger.info(msg="-" * 36)
                status, msgs = self.subprocess_popen(os_cmd + ((" " + os_args) if os_args != '' else ""),
                                                     cwd=self._path[0])
                logger.info(msg="".join(msgs))
                logger.info(msg="-" * 36)
                logger.info(msg="[PyJsEngine]<call_os_cmd>: status <%s>" % repr(status))
                logger.info(msg="[PyJsEngine]<call_os_cmd>: OS command executed! (<%s><%s>)" % (
                    repr(os_cmd), repr(os_args)))

            return str(status) == "0"
        except Exception as e:
            self.internal_exception_handler(tag=tag, args=args, e=e)
            logger.error(msg="[PyJsEngine]<call_os_cmd>: {}".format(str(e)))
            logger.debug(msg="------Traceback------\n" + tb.format_exc())
            self.handle_exceptions(tag, args)
            return False
