# coding=utf-8


import inspect
import chardet as crd
import codecs as cdc
import re
from copy import deepcopy
from threading import Lock

from js2py import EvalJs
from js2py.base import to_python, JsObjectWrapper

from MiniUtils import *


def get_method_name():
    return inspect.stack()[1][3]


class PyJsEngineBase:
    DEFAULT_ENCODING = 'utf-8'
    common_bufsize = 1024

    JS_EXT_NAME = ".js"
    EXT_NAME_SET = {'', JS_EXT_NAME}

    MVAR_PREFIX = "__"
    MVAR_VARS = "__vars__"
    MVAR_SCRIPT_NAME = "__script_name__"
    MVAR_WORKING_DIR = "__working_dir__"

    def __init__(self, logger=None):
        self._logger = logger or get_logger()

        self._script = None
        self._registered_context = {}
        self._path = list()

        self._context = None
        self._context_lock = Lock()

        self.MVAR_SET = {
            self.MVAR_SCRIPT_NAME,
            self.MVAR_WORKING_DIR,
        }

    @property
    def path(self):
        return self._path

    def add_to_path(self, path):
        if isinstance(path, str):
            self._path.insert(0, path)

    @property
    def script(self):
        return self._script

    @property
    def context(self):
        with self._context_lock:
            return self._context

    # 注册上下文（context）
    def register_context(self, context):
        if isinstance(context, dict):
            self._registered_context.update(context)

        # 强制将非MVAR_PREFIX开头的键名改为大写字母开头
        for k in list(self._registered_context.keys()):
            if isinstance(k, str) and not k.startswith(self.MVAR_PREFIX):
                if k != k.capitalize():
                    v = self._registered_context.pop(k)
                    new_k = k.capitalize()
                    self._registered_context[new_k] = v

    # 创建JS上下文对象
    def create_js_context(self):
        with self._context_lock:
            if self._context is None:
                registered_context = self._registered_context
                registered_context[self.MVAR_VARS] = {}
                self._context = EvalJs(context=registered_context, enable_require=True)

    # 加载脚本
    def load(self, script):
        if isinstance(script, str):
            self._script = script
        else:
            raise ValueError("Script must be type 'str'!")

    # 加载脚本（从文件）
    def load_from_file(self, file, encoding=None):
        script = self.read_from_file(file, encoding=encoding)

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
        except:
            pass

        self.load(script)

    # 加载脚本（从字串）
    def load_from_string(self, source):
        script = self.read_from_string(source)
        self.load(script)

    # 读取脚本文件
    def read_from_file(self, file, encoding=None):
        logger = self._logger

        logger.debug(msg="Loading script from file...")
        fp = None
        try:
            if isinstance(file, str):
                c = encoding
                if c is None:
                    # 如无自定义文件编码，先读文件自动检测编码
                    tmpf = open(file=file, mode='rb')
                    tmps = tmpf.read(self.common_bufsize)
                    tmpchd = crd.detect(tmps)
                    logger.debug(msg="Encoding detected: %s" % (repr(tmpchd)))
                    # 再用codecs按检测到的编码读取内容
                    c = tmpchd['encoding']
                fp = cdc.open(file, encoding=c)
            else:
                fp = file

            script = self.read_from_string(fp.read())

            return script
        finally:
            if fp is not None and isinstance(file, str):
                fp.close()

    # 读取脚本数据
    def read_from_string(self, source):
        if isinstance(source, str):
            return source
        elif isinstance(source, bytes):
            return source.decode(self.DEFAULT_ENCODING)

        return None

    # 执行脚本
    def run(self, temp_script=None):
        script = temp_script
        if script is None:
            script = self._script

        self.create_js_context()
        self.context.execute(script)

    def get_vars(self):
        return self.context[self.MVAR_VARS]

    def get_vars_dict(self, vars=None):
        if vars is None:
            # 如未指定vars，则使用内置上下文vars
            vars = self.get_vars()
        if isinstance(vars, JsObjectWrapper):
            # 确保vars为非JsObjectWrapper
            vars = vars.to_dict()
        return vars

    # 解析参数字典（所有参数；仅做类型转换）
    @staticmethod
    def args_parser_all(jskwargs):
        return jskwargs.to_dict() if isinstance(jskwargs, JsObjectWrapper) else jskwargs

    # 解析参数字典，按规则规范参数格式及赋予默认值
    # jskwargs Js传递的参数字典，rules 解析规则字典（格式：{key:(name, type, default)}，type有 s字串/sr原始字串/i整数/r实数/b布尔 ）
    def args_parser(self, jskwargs, rules):
        if not isinstance(rules, dict):
            return None

        if isinstance(jskwargs, JsObjectWrapper):
            # 确保jskwargs为非JsObjectWrapper
            jskwargs = jskwargs.to_dict()

        if not isinstance(jskwargs, dict):
            return None

        args = {}
        keys = set(jskwargs.keys())
        for pk, pv in rules.items():
            if isinstance(pk, str) and isinstance(pv, tuple) and len(pv) in {2, 3}:
                # pk: key, pn: name, pt: type, pd: default
                lpv = len(pv)
                if lpv == 2:
                    pn, pt = pv
                    pd = None
                    if pk not in keys:
                        continue
                else:
                    pn, pt, pd = pv

                value = jskwargs.get(pk, pd)
                if isinstance(value, bytes):
                    value = value.decode(self.DEFAULT_ENCODING)
                if pt in {'s', 'i', 'r', 'b'}:
                    value = self.var_replacer(value) if isinstance(value, str) else value
                if pt in {'s', 'sr'}:
                    args[pn] = str(value) if value is not None else None
                elif pt in {'i', 'ir'}:
                    args[pn] = (int(eval(str(value))) if value is not None else None) if not isinstance(value,
                                                                                                        int) else value
                elif pt in {'r', 'rr'}:
                    args[pn] = (eval(str(value)) if value is not None else None) if not isinstance(value,
                                                                                                   float) else value
                elif pt in {'b', 'br'}:
                    args[pn] = str(value).lower() == str(True).lower()

        if self.MVAR_VARS not in args:
            try:
                args[self.MVAR_VARS] = self.get_vars_dict()
            except:
                pass

        return args

    # 变量标识替换
    def var_replacer(self, v_str, vars=None, v_prefix=r"$%", v_suffix=r"%$", re_prefix=r"\$\%", re_suffix=r"\%\$"):
        vars = self.get_vars_dict(vars=vars)
        return self.var_replacer_raw(vars, v_str,
                                     v_prefix=v_prefix, v_suffix=v_suffix, re_prefix=re_prefix,
                                     re_suffix=re_suffix)

    # 变量标识替换（原始方法）
    @staticmethod
    def var_replacer_raw(var_dict, v_str, v_prefix=r"$%", v_suffix=r"%$", re_prefix=r"\$\%", re_suffix=r"\%\$"):
        keys = re.findall(re_prefix + r"(.+?)" + re_suffix, v_str)
        d_keys = []
        for i in keys:
            value = var_dict.get(i)
            if value is not None:
                d_keys.append(i)

        o_str = deepcopy(v_str)
        for i in d_keys:
            # 注：这里var_dict.get(i)需要强制转字符串，适应var_dict中保存包含字符串以外类型对象的情况
            o_str = o_str.replace(v_prefix + i + v_suffix, str(var_dict.get(i)))
        return o_str
