# coding=utf-8


import traceback as tb
import subprocess
import types
import csv
from functools import partial
from threading import Lock
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from time import strftime, strptime, time, localtime, sleep
from copy import deepcopy
# from collections import ChainMap

from jinja2 import Environment, ChoiceLoader, FileSystemLoader, Template

from .PyJsEngineBase import PyJsEngineBase, get_method_name
from .MiniUtils import *

__version__ = "1.0.191024"


class PyJsEngine(PyJsEngineBase):
    funcn_msg = "msg"
    funcn_sub = "sub"
    funcn_assert = "assert"
    funcn_assert_not = "assert_not"
    funcn_module = "module"
    funcn_procedure = "procedure"
    funcn_procedure_privilege = "procedure_privilege"
    funcn_call = "call"
    funcn_cond_call = "cond_call"
    funcn_set = "set"
    funcn_set_int = "set_int"
    funcn_set_num = "set_num"
    funcn_set_str = "set_str"
    funcn_set_eval = "set_eval"
    funcn_get = "get"
    funcn_load_vars = "load_vars"
    funcn_load_data = "load_data"
    funcn_output = "output"
    funcn_template = "template"
    funcn_call_os_cmd = "call_os_cmd"

    attrn_msg = "msg"
    attrn_name = "name"
    attrn_src = "src"
    attrn_key = "key"
    attrn_value = "value"
    attrn_default_value = "defvalue"
    attrn_type = "type"
    attrn_cols = "cols"
    attrn_auto_strip = "auto_strip"
    attrn_allow_none = "allow_none"
    attrn_mode = "mode"
    attrn_newline = "newline"
    attrn_encoding = "encoding"
    attrn_content = "content"
    attrn__from_file = "_from_file"
    attrn__to_file = "_to_file"
    attrn_cmd = "cmd"
    attrn_args = "args"

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
    MVAR_LOG_DATETIME = "__log_datetime__"

    PREPARE_SCRIPT_MAIN = r"""
        function Next_(iterator) {
            var i;
            try {
                i = Next(it);
            } catch (e){
                i = null;
            }
            return i;
        }
    """

    def __init__(self, logger=None, msg_handler=None, **kwargs):
        # 父类初始化
        super().__init__(logger=logger, msg_handler=msg_handler, **kwargs)
        # 更新MVAR集合
        self.MVAR_SET.update({
            self.MVAR_LOADED_DATA_COUNT,
            self.MVAR_LOADED_DATA_ITEM_INDEX,
            self.MVAR_LOADED_DATA_COLS,
            self.MVAR_LOG_DATETIME,
        })
        # 用于存放过程
        self._proc_dict = dict()
        # 其他变量
        self._encoding = self.DEFAULT_ENCODING
        if msg_handler is None:
            def handler(msg, **kwargs):
                self._logger.info(msg=msg)

            msg_handler = handler
        self._msg_handler = msg_handler

        # 2019-5-5
        self._j2_env = None  # Jinja2 environment

        # 2019-9-21
        self._output_lmap = {}
        self._output_lmap_lock = Lock()

        self.register_context({
            # ---- Engine functions ----
            self.funcn_msg: self.run_msg,
            self.funcn_sub: self.run_sub,
            self.funcn_assert: self.run_assert,
            self.funcn_assert_not: self.run_assert_not,
            self.funcn_module: self.run_module,
            self.funcn_procedure: self.run_procedure,
            self.funcn_call: self.run_call,
            self.funcn_cond_call: self.run_cond_call,
            self.funcn_set: self.run_set,
            self.funcn_set_int: partial(self.run_set_with_type, funcn=self.funcn_set_int),
            self.funcn_set_num: partial(self.run_set_with_type, funcn=self.funcn_set_num),
            self.funcn_set_str: partial(self.run_set_with_type, funcn=self.funcn_set_str),
            self.funcn_set_eval: partial(self.run_set_with_type, funcn=self.funcn_set_eval),
            self.funcn_get: self.run_get,
            self.funcn_load_vars: self.run_load_vars,
            self.funcn_load_data: self.run_load_data,
            self.funcn_output: self.run_output,
            self.funcn_template: self.run_template,
            self.funcn_call_os_cmd: self.run_call_os_cmd,
            # ---- Python functions / modules ----
            "sleep": sleep,
            "json": json,
            "csv": csv,
            "open": open,
            "len": len,
            "datetime": datetime,
            "timezone": timezone,
            "relativedelta": relativedelta,
            "time": time,
            "localtime": localtime,
            "strftime": strftime,
            "strptime": strptime,
            "iter": iter,
            "next": next,
        })
        # self._logger.debug(msg="registered_context={}".format(repr(self._registered_context)))  # debug
        self.append_prepare_script(self.PREPARE_SCRIPT_MAIN)

        self._logger.debug(msg="PyJsEngine loaded. ({})".format(__version__))

    @property
    def encoding(self):
        return self._encoding

    @property
    def msg_handler(self):
        return self._msg_handler

    @msg_handler.setter
    def msg_handler(self, mh):
        self._msg_handler = mh

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
                logger.debug(msg="[PyJsEngine]<{}>: Jinja2 Environment initialized.".format(get_method_name()))
            except Exception as e:
                self._j2_env = None
                self.internal_exception_handler(funcn=get_method_name(), e=e)

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

    # 加载脚本（从文件）
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
                vars = self.get_vars()
                vars[self.MVAR_SCRIPT_NAME] = os.path.splitext(base_name)[0]
                vars[self.MVAR_WORKING_DIR] = dirname
                self._path.clear()
                self.add_to_path(os.path.abspath('.'))  # 最后：程序目录
                self.add_to_path(dirname)  # 倒数第二：脚本目录
                self.init_jinja2_env()
            except:
                pass

        self.load(script, load_as_module=load_as_module)

    # 加载脚本（从字串）
    def load_from_string(self, source, load_as_module=False):
        script = self.read_from_string(source)
        self.load(script, load_as_module=load_as_module)

    def send_msg_to_handler(self, msg, **kwargs):
        logger = self._logger

        try:
            if not self._msg_handler or not msg:
                return

            # logger.info(msg="[PyJsEngine]<{}>: {}".format(get_method_name(), msg))
            self._msg_handler(msg, **kwargs)
        except:
            pass

    # 脚本内部异常处理（可被覆写）
    def internal_exception_handler(self, funcn=None, jskwargs=None, args=None, e=None, ignore_err=False):
        logger = self._logger
        logger.error(msg="[PyJsEngine]<{}>: {}".format(funcn, str(e)))
        logger.debug(msg="------Traceback------\n" + tb.format_exc())
        if e is not None and not ignore_err:
            raise e

    # 生成日志时间
    def generate_log_datetime(self, jskwargs, *args):
        _log_datetime = strftime("%Y-%m-%d %H:%M:%S")
        vars = self.get_vars()
        vars[self.MVAR_LOG_DATETIME] = _log_datetime

        return _log_datetime

    # 生成load_data进度
    def generate_load_data_progress(self, jskwargs, *args):
        vars_dict = self.get_vars_dict()
        _index = vars_dict.get(self.MVAR_LOADED_DATA_ITEM_INDEX, 0)
        _count = vars_dict.get(self.MVAR_LOADED_DATA_COUNT, 0)
        try:
            if int(_count) > 0:
                return str(_index) + '/' + str(_count)
        except:
            pass

        return None

    def do_before(self, jskwargs, *args):
        super().do_before(jskwargs, *args)

        jargs = self.args_parser(jskwargs, {
            self.attrn_msg: ('msg', 's', None),
        })
        msg = jargs['msg']

        self.generate_log_datetime(jskwargs, *args)
        if msg:
            progress = self.generate_load_data_progress(jskwargs, *args)
            msg = (('(' + progress + ') ') if progress else '') + msg
            self.send_msg_to_handler(msg)

    def args_parser(self, jskwargs, rules):
        if isinstance(jskwargs, str):
            # 兼容第一参数为字符串类型的操作
            jskwargs = {
                self.attrn_key: jskwargs,
            }
        return super().args_parser(jskwargs, rules)

    def wrapped_method(self, jskwargs, *args, func=None):
        if func == self.run_msg and isinstance(jskwargs, str):
            # 兼容第一参数为字符串类型的操作
            jskwargs = {
                self.attrn_msg: jskwargs,
            }
        return super().wrapped_method(jskwargs, *args, func=func)

    # 操作：消息 msg
    def run_msg(self, jskwargs, *args):
        # 已在do_before()中处理，这里不再重复输出
        pass

    # 操作：子程序（忽略调用的子函数中的错误）
    def run_sub(self, jskwargs, *args):
        logger = self._logger

        # 属性
        # jargs = self.args_parser(jskwargs, {
        # })

        # 处理
        try:
            logger.debug(msg="[PyJsEngine]<{}>: sub-script entered.".format(get_method_name()))
            result = args[0]()
            logger.debug(
                msg="[PyJsEngine]<{}>: sub-script exited (result={}).".format(get_method_name(), format(result)))
            return result
        except Exception as e:
            self.internal_exception_handler(funcn=get_method_name(), jskwargs=jskwargs, args=args, e=e, ignore_err=True)
            return None

    # 操作：断言
    def run_assert(self, jskwargs, *args):
        logger = self._logger

        # 属性
        jargs = self.args_parser(jskwargs, {
            self.attrn_key: ('key', 's', None),
            self.attrn_value: ('value', 's', None)
        })
        key = (jargs['key'] or '').strip()
        value = jargs['value']

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
            vars_dict = self.get_vars_dict()
            _value = None
            if isinstance(key, str) and key != '':
                # 检查变量值
                _value = vars_dict.get(key)
            else:
                _value = args[0]()
                if not my_assert(_value, value):
                    return False
                return True
            logger.debug(
                "[PyJsEngine]<{}>: assert value: {} (compare with value: {})".format(get_method_name(),
                                                                                     repr(_value), repr(value)))

            return my_assert(_value, value)
        except Exception as e:
            self.internal_exception_handler(funcn=get_method_name(), jskwargs=jskwargs, args=args, e=e)

    # 操作：否定断言
    def run_assert_not(self, jskwargs, *args):
        logger = self._logger

        # 属性
        jargs = self.args_parser(jskwargs, {
            self.attrn_key: ('key', 's', None),
            self.attrn_value: ('value', 's', None)
        })
        key = (jargs['key'] or '').strip()
        value = jargs['value']

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
            vars_dict = self.get_vars_dict()
            _value = None
            if isinstance(key, str) and key != '':
                # 检查变量值
                _value = vars_dict.get(key)
            else:
                _value = args[0]()
                if not my_assert_not(_value, value):
                    return False
                return True
            logger.debug(
                "[PyJsEngine]<{}>: assert value: {} (compare with value: {})".format(get_method_name(),
                                                                                     repr(_value), repr(value)))

            return my_assert_not(_value, value)
        except Exception as e:
            self.internal_exception_handler(funcn=get_method_name(), jskwargs=jskwargs, args=args, e=e)

    # 操作：预加载模块
    def run_module(self, jskwargs, *args):
        logger = self._logger

        # 属性
        jargs = self.args_parser(jskwargs, {
            self.attrn_src: ('src', 's', None)
        })
        src = jargs['src']
        try:
            # 特别操作：按顺序检查不加扩展名、加扩展名是否能找到；其中每次按path顺序检查是否能找到
            for ext in self.EXT_NAME_SET:
                esrc = src + ext
                for dir in self._path:
                    nsrc = os.path.join(dir, esrc)
                    if os.path.exists(nsrc):
                        src = nsrc
                        break
        except:
            pass

        # 处理
        try:
            self.load_from_file(src, load_as_module=True)
        except Exception as e:
            self.internal_exception_handler(funcn=get_method_name(), jskwargs=jskwargs, args=args, e=e)

    # 操作：定义过程
    def run_procedure(self, jskwargs, *args):
        logger = self._logger

        # 属性
        jargs = self.args_parser(jskwargs, {
            self.attrn_name: ('proc_name', 'sr', None)
        })
        proc_name = (jargs['proc_name'] or '').strip()

        # 处理
        try:
            context = self.context
            proc_dict = self._proc_dict
            func = args[0]
            if isinstance(proc_name, str) and proc_name != '':
                proc_names = proc_name.split(',')
                for proc_name in proc_names:
                    proc_dict[proc_name] = func
                    context[proc_name.capitalize()] = func

                logger.debug(msg="[PyJsEngine]<{}>: proc_names={} defined.".format(get_method_name(), repr(proc_names)))
            else:
                raise ValueError("proc_name={} name illegal!".format(repr(proc_name)))
        except Exception as e:
            self.internal_exception_handler(funcn=get_method_name(), jskwargs=jskwargs, args=args, e=e)

    # 操作：调用过程
    def run_call(self, jskwargs, *args):
        logger = self._logger

        # 属性
        jargs = self.args_parser(jskwargs, {
            self.attrn_name: ('proc_name', 's', None),
        })
        proc_name = (jargs['proc_name'] or '').strip()

        # 处理
        try:
            proc_dict = self._proc_dict
            if isinstance(proc_name, str) and proc_name != '':
                func = proc_dict.get(proc_name)
                if func is not None:
                    logger.debug(msg="[PyJsEngine]<{}>: proc_name={} found!".format(get_method_name(), repr(proc_name)))

                    # 传递参数至要call的procedure
                    new_jargs = deepcopy(jargs)
                    for k in {self.attrn_name}:
                        if k in new_jargs:
                            new_jargs.pop(k)

                    # 进入procedure子标签调用
                    return func(jskwargs, *args)
                else:
                    raise RuntimeError("proc_name={} not found!".format(repr(proc_name)))
            else:
                raise ValueError("proc_name={} name illegal!".format(repr(proc_name)))
        except Exception as e:
            self.internal_exception_handler(funcn=get_method_name(), jskwargs=jskwargs, args=args, e=e)

    # 操作：调用过程（根据变量条件）
    def run_cond_call(self, jskwargs, *args):
        logger = self._logger

        # 属性
        jargs = self.args_parser(jskwargs, {
            self.attrn_name: ('proc_name', 's', None),
        })
        proc_name = (jargs['proc_name'] or '').strip()

        # 处理
        try:
            vars_dict = self.get_vars_dict()
            # proc_dict = self._proc_dict
            if isinstance(proc_name, str) and proc_name != '':
                proc_switcher = vars_dict.get(self.PROC_SWITCHER_PREFIX + proc_name)
                proc_switcher = str(proc_switcher) if proc_switcher is not None else None
                if proc_switcher == self.PROC_SWITCHER_FLAG:
                    # 直接调用原run_call()方法
                    self.run_call(jskwargs, *args)
            else:
                raise ValueError("proc_name={} name illegal!".format(repr(proc_name)))
        except Exception as e:
            self.internal_exception_handler(funcn=get_method_name(), jskwargs=jskwargs, args=args, e=e)

    # 操作：渲染模板
    def run_template(self, jskwargs, *args):
        logger = self._logger

        # 属性
        jargs = self.args_parser(jskwargs, {
            self.attrn_key: ('key', 's', None),
            self.attrn__from_file: ('_from_file', 's', None),
            self.attrn__to_file: ('_to_file', 's', None),
            self.attrn_content: ('content', 's', None),
            self.attrn_encoding: ('encoding', 's', None),
        })
        key = (jargs['key'] or '').strip()
        _from_file = jargs['_from_file']
        _to_file = jargs['_to_file']
        _to_key = jargs['_to_key']
        content = jargs['content']
        encoding = jargs['encoding'] or self._encoding

        # 处理
        try:
            vars_dict = self.get_vars_dict()
            fp = None
            template_str = None
            template = None
            if isinstance(key, str) and key != '':
                template_str = self.engine_get_var(vars_dict, key)
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
                        logger.debug("[PyJsEngine]<{}>: Template loaded with env.".format(get_method_name()))
                    except Exception as e:
                        template = None
                        logger.debug("[PyJsEngine]<{}>: Loading template with env failed! ({})".format(
                            get_method_name(),
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
                logger.debug("[PyJsEngine]<{}>: template_str={}".format(get_method_name(), repr(template_str)))
                template = Template(template_str)

            vars_dict = self.get_vars_dict()
            template_render_result = template.render(**vars_dict)
            # logger.debug("[PyJsEngine]<{}>: template_render_result={}".format(get_method_name(), repr(template_render_result)))
            logger.debug("[PyJsEngine]<{}>: len(template_render_result)={}".format(
                get_method_name(),
                len(template_render_result) if template_render_result is not None else -1))

            # 输出
            if _to_file is not None:
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
            return template_render_result
        except Exception as e:
            self.internal_exception_handler(funcn=get_method_name(), jskwargs=jskwargs, args=args, e=e)

    # 操作：定义变量
    def run_set(self, jskwargs, *args):
        logger = self._logger

        # 属性
        jargs = self.args_parser(jskwargs, {
            self.attrn_type: ('set_type', 's', self.SET_TYPE_VALUE),
            self.attrn_key: ('key', 's', None),
            self.attrn_value: ('value', 's', None),
        })
        set_type = jargs['set_type']
        key = (jargs['key'] or '').strip()
        value = jargs['value'] or None

        # 处理
        try:
            vars = self.get_vars()
            if isinstance(key, str) and key != '':
                logger.debug(
                    "[PyJsEngine]<{}>: setting {}({})...".format(get_method_name(), repr(key), repr(set_type)))
                if set_type == self.SET_TYPE_OBJECT:
                    value = args[0]()
                    vars[key] = value
                elif set_type == self.SET_TYPE_EVAL:
                    value = eval(value)
                    vars[key] = value
                elif set_type == self.SET_TYPE_VALUE:
                    vars[key] = value
                else:
                    raise ValueError("set_type illegal!")
                logger.debug("[PyJsEngine]<{}>: value={}, type(value)={}".format(get_method_name(),
                                                                                 repr(value), repr(type(value))))
            else:
                raise ValueError("key name illegal!")
        except Exception as e:
            self.internal_exception_handler(funcn=get_method_name(), jskwargs=jskwargs, args=args, e=e)

    # 操作：定义变量（带类型）
    def run_set_with_type(self, jskwargs, *args, funcn=None):
        logger = self._logger

        # 属性
        jargs = self.args_parser_all(jskwargs)

        # 处理
        try:
            vars = self.get_vars()
            tmp_dict = {}
            for k, v in jargs.items():
                if funcn in {self.funcn_set_int, self.funcn_set_num, self.funcn_set_str, }:
                    v = self.var_replacer(str(v), vars=vars) if v is not None else None
                    # v = self.var_replacer(v, vars=vars) if isinstance(v, str) else None
                    # v = self.var_replacer(v, vars=vars) if v is not None else None
                    if funcn == self.funcn_set_int:
                        try:
                            v = int(v)
                        except ValueError:
                            v = int(float(v))
                    elif funcn == self.funcn_set_num:
                        v = float(v)
                    elif funcn == self.funcn_set_str:
                        v = str(v)
                elif funcn in {self.funcn_set_eval}:
                    v = eval(v)
                else:
                    raise RuntimeError()
                tmp_dict[k] = v
                logger.debug("[PyJsEngine]<{}>: preparing {} = {} ...".format(get_method_name(), repr(k), repr(v)))
            # 检查到全部变量定义不存在问题，才执行update进行更新
            for k, v in tmp_dict.items():
                vars[k] = v
        except Exception as e:
            self.internal_exception_handler(funcn=get_method_name(), jskwargs=jskwargs, args=args, e=e)

    # 操作：取得变量
    def run_get(self, jskwargs, *args):
        logger = self._logger

        # 属性
        jargs = self.args_parser(jskwargs, {
            self.attrn_key: ('key', 's', None),
            self.attrn_default_value: ('defvalue', 's', None),
        })
        key = (jargs['key'] or '').strip()
        defvalue = jargs['defvalue']

        # 处理
        try:
            vars_dict = self.get_vars_dict()
            # vars_dict = self.get_vars_dict()
            if isinstance(key, str) and key != '':
                logger.debug(
                    "[PyJsEngine]<{}>: getting {}...".format(get_method_name(), repr(key)))
                value = vars_dict.get(key, defvalue)
                logger.debug("[PyJsEngine]<{}>: value={}, type(value)={}".format(get_method_name(),
                                                                                 repr(value), repr(type(value))))

                return value
            elif defvalue is not None:
                return defvalue
            else:
                raise ValueError("key name illegal!")
        except Exception as e:
            self.internal_exception_handler(funcn=get_method_name(), jskwargs=jskwargs, args=args, e=e)

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
    def run_load_vars(self, jskwargs, *args):
        logger = self._logger

        # 属性
        jargs = self.args_parser(jskwargs, {
            self.attrn_type: ('file_type', 's', None),
            self.attrn_name: ('file_name', 's', None),
            self.attrn_encoding: ('encoding', 's', None),
            self.attrn_auto_strip: ('auto_strip', 'b', True),
            self.attrn_allow_none: ('allow_none', 'b', False)
        })
        file_type = jargs['file_type'] or self.FILE_TYPE_CSV
        file_name = (jargs['file_name'] or '')  # .strip()
        encoding = jargs['encoding'] or self._encoding
        auto_strip = jargs['auto_strip']
        allow_none = jargs['allow_none']
        try:
            for dir in self._path:
                n_file_name = os.path.join(dir, file_name)
                if os.path.exists(n_file_name):
                    file_name = n_file_name
                    break
        except:
            pass

        # 处理
        try:
            vars = self.get_vars()
            logger.debug(msg="[PyJsEngine]<{}>: loading vars started! ({})".format(get_method_name(), file_name))
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
                                    msg="[PyJsEngine]<{}>: skipping empty row... ({})".format(
                                        get_method_name(),
                                        str(nr + 1)))
                                continue

                            # 处理变量名
                            var_name = r[0]
                            if not var_name:
                                logger.info(
                                    msg="[PyJsEngine]<{}>: skipping empty var name... ({})".format(
                                        get_method_name(),
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
                            vars[var_name] = var_value
                            logger.info(
                                msg="[PyJsEngine]<{}>: var stored! ({} -> {}) ({})".format(
                                    get_method_name(),
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
            logger.debug(msg="[PyJsEngine]<{}>: loading vars finished! ({})".format(get_method_name(), file_name))
        except Exception as e:
            self.internal_exception_handler(funcn=get_method_name(), jskwargs=jskwargs, args=args, e=e)

    def load_data_file(self, file_name, file_type=FILE_TYPE_CSV, encoding=None, **kwargs):
        logger = self._logger
        encoding = encoding or self._encoding
        data_list = []
        fieldnames = []

        if file_type == self.FILE_TYPE_CSV:
            auto_strip = kwargs.get('auto_strip')
            allow_none = kwargs.get('allow_none')

            count = max(self.count_file_lines(file_name, encoding=encoding) - 1, 0)
            logger.info(msg="[PyJsEngine]<{}>: lines count: {}".format(get_method_name(), str(count)))

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
                            msg="[PyJsEngine]<{}>: skipping empty row... ({}/{})".format(
                                get_method_name(),
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
    def run_load_data(self, jskwargs, *args):
        logger = self._logger

        # 属性
        jargs = self.args_parser(jskwargs, {
            self.attrn_type: ('file_type', 's', None),
            self.attrn_name: ('file_name', 's', None),
            self.attrn_encoding: ('encoding', 's', None),
            self.attrn_auto_strip: ('auto_strip', 'b', True),
            self.attrn_allow_none: ('allow_none', 'b', False)
        })
        file_type = jargs['file_type'] or self.FILE_TYPE_CSV
        file_name = (jargs['file_name'] or '')  # .strip()
        encoding = jargs['encoding'] or self._encoding
        auto_strip = jargs['auto_strip']
        allow_none = jargs['allow_none']
        try:
            for dir in self._path:
                n_file_name = os.path.join(dir, file_name)
                if os.path.exists(n_file_name):
                    file_name = n_file_name
                    break
        except:
            pass

        # 处理
        try:
            logger.debug(msg="[PyJsEngine]<{}>: loading data started! ({})".format(get_method_name(), file_name))
            if isinstance(file_name, str) and file_name != '':
                data_list, fieldnames, count = self.load_data_file(file_name, file_type=file_type, encoding=encoding,
                                                                   auto_strip=auto_strip, allow_none=allow_none)
                logger.info(msg="[PyJsEngine]<{}>: lines count: {}".format(get_method_name(), str(count)))
                logger.info(msg="[PyJsEngine]<{}>: fieldnames: {}".format(get_method_name(), repr(fieldnames)))

                new_data_list = []
                for nd, d in enumerate(data_list):
                    # index从1计起，适应实际需求
                    new_ind = nd + 1
                    d[self.MVAR_LOADED_DATA_ITEM_INDEX] = new_ind
                    d[self.MVAR_LOADED_DATA_COUNT] = count

                    d[self.MVAR_LOADED_DATA_COLS] = ",".join(fieldnames)
                    # logger.debug(
                    #     msg="[PyJsEngine]<{}>: row read! ({}/{})".format(get_method_name(), str(nd + 1), str(count)))
                    new_data_list.append(d)

                logger.info(msg="[PyJsEngine]<{}>: loading data finished! ({})".format(get_method_name(), file_name))
                return iter(new_data_list)
            else:
                raise ValueError("file name illegal!")
        except Exception as e:
            self.internal_exception_handler(funcn=get_method_name(), jskwargs=jskwargs, args=args, e=e)

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
    def run_output(self, jskwargs, *args):
        logger = self._logger

        # 属性
        jargs = self.args_parser(jskwargs, {
            self.attrn_type: ('file_type', 's', None),
            self.attrn_name: ('file_name', 's', None),
            self.attrn_encoding: ('encoding', 's', None),
            self.attrn_newline: ('newline', 's', None),
            self.attrn_mode: ('mode', 's', None),
            self.attrn_cols: ('cols', 's', None)
        })
        file_type = jargs['file_type'] or self.FILE_TYPE_CSV
        file_name = (jargs['file_name'] or '')  # .strip()
        encoding = jargs['encoding'] or self._encoding
        newline = jargs['newline']
        mode = jargs['mode'] or 'a'  # output方式默认为append
        cols = (jargs['cols'] or '').strip()
        cols = [c.strip() for c in cols.split(',')]
        try:
            n_file_name = os.path.join(self._path[0], file_name)
            file_name = n_file_name
        except:
            pass

        # 处理
        try:
            logger.debug(msg="[PyJsEngine]<{}>: output started!".format(get_method_name()))
            if isinstance(file_name, str) and file_name != '':
                with self.get_output_lock(file_name):
                    if len(args) > 0:
                        # 子标签第1个标签返回的结果作为文件数据写入到文件（无视输出文件类型）
                        data = args[0]()
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
                                    vars_dict = self.get_vars_dict()
                                    row = {}
                                    for c in cols:
                                        # 改成取连同全局变量在内的变量
                                        row[c] = vars_dict.get(c)
                                    cdw.writerow(row)
                                    logger.info(msg="[PyJsEngine]<{}>: row written!".format(get_method_name()))
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
            logger.debug(msg="[PyJsEngine]<{}>: output finished!".format(get_method_name()))
        except Exception as e:
            self.internal_exception_handler(funcn=get_method_name(), jskwargs=jskwargs, args=args, e=e)

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
    def run_call_os_cmd(self, jskwargs, *args):
        logger = self._logger

        # 属性
        jargs = self.args_parser(jskwargs, {
            self.attrn_cmd: ('os_cmd', 's', ''),
            self.attrn_args: ('os_args', 's', ''),
        })
        os_cmd = jargs['os_cmd']
        os_args = jargs['os_args']

        # 处理
        try:
            status = None
            if os_cmd != '':
                logger.info(msg="[PyJsEngine]<{}>: Executing OS command... (<{}><{}>)".format(
                    get_method_name(),
                    repr(os_cmd), repr(os_args)))
                logger.info(msg="-" * 36)
                status, msgs = self.subprocess_popen(os_cmd + ((" " + os_args) if os_args != '' else ""),
                                                     cwd=(self._path[0] if len(self._path) > 0 else None))
                logger.info(msg="".join(msgs))
                logger.info(msg="-" * 36)
                logger.info(msg="[PyJsEngine]<{}>: status <{}>".format(get_method_name(), repr(status)))
                logger.info(msg="[PyJsEngine]<{}>: OS command executed! (<{}><{}>)".format(
                    get_method_name(),
                    repr(os_cmd), repr(os_args)))

            return str(status) == "0"
        except Exception as e:
            self.internal_exception_handler(funcn=get_method_name(), jskwargs=jskwargs, args=args, e=e)


if __name__ == "__main__":
    engine = PyJsEngine()
    engine.run(temp_script=r"""
        console.log("Python + Js2Py");
        var it = Load_data({ name: "C:\\temp\\test.csv", encoding: "gbk", msg: "Loading csv file...", })
        for (var i = Next_(it); i; ){
            Set_str(i);
            Msg(Get("PassengerId") + ", " + Get("Name"));
            Output({
                name: "C:\\temp\\test_output.csv",
                encoding: "gbk",
                cols: "$%__loaded_data_cols__%$"
            });
            i = Next_(it);
        }
    """)
