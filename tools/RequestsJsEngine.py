# coding=utf-8

import requests
from functools import partial
from queue import Queue, Empty

from PyJsEngine import PyJsEngine, get_method_name

__version__ = "1.0.191029"


class RequestsJsEngine(PyJsEngine):
    DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.75 Safari/537.36"
    DEFAULT_HEADERS = {
        "User-Agent": DEFAULT_USER_AGENT,
    }
    DEFAULT_REQUEST_TIMEOUT = 10
    DEFAULT_ENCODING = "utf-8"
    DEFAULT_INPUT_TIMEOUT = 60

    funcn_rget = "rget"
    funcn_rpost = "rpost"
    funcn_session = "session"  # TODO
    funcn_cookies = "cookies"  # TODO
    funcn_input = "input"
    funcn_ok = "ok"  # TODO 标记任务完成
    funcn_err = "err"  # TODO 标记任务失败

    attrn_session = "session"
    attrn_cookies = "cookies"
    attrn_url = "url"
    attrn_headers = "headers"
    attrn_data = "data"
    attrn_timeout = "timeout"
    # attrn_encoding = "encoding"
    # attrn_get_bytes = "get_bytes"

    PREPARE_SCRIPT_REQUESTS_JS = r"""
        function Data() {
            this._data = {};
            if (arguments.length > 0)
                var obj = arguments[0];
                this.Update_data(obj);
        }
        
        Data.prototype.Update_data = function(obj) {
            if (obj) {
                for (var i in obj) {
                    this._data[i] = obj[i];
                }
            }
        }
        
        Data.prototype.Gather_data = function() {
            if (arguments.length > 0) {
                var fields = arguments[0];
                var data = {};
                for (var i in fields) {
                    var key = fields[i];
                    data[key] = this._data[key];
                }
                return data;
            } else {
                return this._data;
            }
        }
        
        
        function Get_(url) {
            var headers = null;
            if (arguments.length > 1){
                headers = arguments[1];
            }
            
            return Rget({
                url: url,
                headers: headers != null? JSON.stringnify(headers): null,
            });
        }
        
        function Post_(url) {
            var data = null;
            var headers = null;
            if (arguments.length > 1){
                data = arguments[1];
            }
            if (arguments.length > 2){
                headers = arguments[2];
            }
            
            return Rpost({
                url: url,
                data: data != null? JSON.stringnify(data): null,
                headers: headers != null? JSON.stringnify(headers): null,
            });
        }
    """

    def __init__(self, logger=None, msg_handler=None, **kwargs):
        super().__init__(logger=logger, msg_handler=msg_handler, **kwargs)

        self._session = requests.session()
        self._cookies = requests.cookies.RequestsCookieJar()
        self._session.cookies = self._cookies

        self._cookies_str = None
        self._queue = Queue()

        self._input_timeout = kwargs.get("input_timeout", self.DEFAULT_INPUT_TIMEOUT)

        # Register runners for Requests and js2py
        self.register_context({
            # ---- Engine functions ----
            self.funcn_rget: partial(self.run_rfunc, funcn=self.funcn_rget),
            self.funcn_rpost: partial(self.run_rfunc, funcn=self.funcn_rpost),
            self.funcn_input: self.run_input,
            # ---- Constants ----
            "user_agent": self.DEFAULT_USER_AGENT,
        })
        self.append_prepare_script(self.PREPARE_SCRIPT_REQUESTS_JS)

        self._logger.debug(msg="RequestsJsEngine loaded. ({})".format(__version__))

    @property
    def cookies(self):
        return self._cookies

    @property
    def cookies_str(self):
        self._cookies_str = self.get_cookies_str(self._cookies)
        return self._cookies_str

    @cookies_str.setter
    def cookies_str(self, cstr):
        cl = self.get_cookies_list_from_str(cstr)
        self.set_cookies_list(self._cookies, cl)

    @property
    def cookies_dict(self):
        return self.get_cookies_dict(self._cookies)

    @property
    def cookies_list(self):
        return self.get_cookies_list(self._cookies)

    @staticmethod
    def get_cookies_str(cookies):
        # l = ['%s=%s' % (i.name, i.value) for i in cookies]  # MozillaCookieJar
        l = ['%s=%s' % (name, value) for name, value in cookies.items()]  # RequestsCookieJar
        cookie_str = ';'.join(l)
        return cookie_str

    @staticmethod
    def get_cookies_list_from_str(cookies_str):
        result = []
        if isinstance(cookies_str, str):
            args = cookies_str.split("&")
            for a in args:
                sa = a.split("=")
                result.append({
                    "name": sa[0],
                    "value": sa[1],
                })
        return result

    @staticmethod
    def get_cookies_dict(cookies):
        # return {i.name: i.value for i in cookies}  # MozillaCookieJar
        return {name: value for name, value in cookies.items()}  # RequestsCookieJar

    @staticmethod
    def get_cookies_list(cookies):
        return [{
            "version": i.version,
            "name": i.name,
            "value": i.value,
            "path": i.path,
            "port": i.port,
            "domain": i.domain,
            "secure": i.secure,
            "expires": i.expires,
            "discard": i.discard,
        } for i in iter(cookies)]  # RequestsCookieJar

    @staticmethod
    def set_cookies_list(cookies, cookies_list):
        for i in cookies_list:
            cookies.set(**i)  # RequestsCookieJar

    @property
    def queue(self):
        return self._queue

    def queue_put(self, item):
        self._queue.put_nowait(item)

    def session_get(self, url, headers=None, timeout=DEFAULT_REQUEST_TIMEOUT):
        if headers is None:
            headers = self.DEFAULT_HEADERS
        session = self._session

        # 获取页面数据
        req = session.get(url, headers=headers, timeout=timeout)
        return req

    def session_post(self, url, headers=None, data=None, timeout=DEFAULT_REQUEST_TIMEOUT):
        if headers is None:
            headers = self.DEFAULT_HEADERS
        session = self._session

        # 获取页面数据
        req = session.post(url, headers=headers, data=data, timeout=timeout)
        return req

    def run_rfunc(self, jskwargs, *args, funcn=None):
        logger = self._logger

        # 属性
        jargs = self.args_parser(jskwargs, {
            self.attrn_url: ('url', 's', None),
            self.attrn_data: ('data', 's', None),
            self.attrn_headers: ('headers', 's', None),
            self.attrn_timeout: ('timeout', 'r', None),
        })
        url = jargs['url']
        data = jargs['data']
        headers = jargs['headers'] or self.DEFAULT_HEADERS
        timeout = jargs['timeout'] or self.DEFAULT_REQUEST_TIMEOUT

        try:
            if funcn == self.funcn_rget:
                logger.debug(
                    msg="[RequestsJsEngine]<{}>: Requests-get url={}, headers={}".format(get_method_name(), url,
                                                                                         headers))
                self.session_get(url, headers=headers, timeout=timeout)
                logger.info(msg="[RequestsJsEngine]<{}>: {}".format(get_method_name(), "Requests-get finished!"))
            elif funcn == self.funcn_rpost:
                logger.debug(
                    msg="[RequestsJsEngine]<{}>: Requests-post url={}, data={}, headers={}".format(get_method_name(),
                                                                                                   url, data, headers))
                self.session_post(url, headers=headers, data=data, timeout=timeout)
                logger.info(msg="[RequestsJsEngine]<{}>: {}".format(get_method_name(), "Requests-post finished!"))
        except Exception as e:
            self.internal_exception_handler(funcn=get_method_name(), jskwargs=jskwargs, args=args, e=e)

    def run_input(self, jskwargs, *args, funcn=None):
        logger = self._logger

        # 属性
        # jargs = self.args_parser(jskwargs, {
        # })

        try:
            try:
                code = self._queue.get(timeout=self._input_timeout)
            except Empty:
                logger.debug(
                    msg="[RequestsJsEngine]<{}>: {}".format(get_method_name(), "Waiting for inputing code timeout!"))
                code = "#"
            return code
        except Exception as e:
            self.internal_exception_handler(funcn=get_method_name(), jskwargs=jskwargs, args=args, e=e)
