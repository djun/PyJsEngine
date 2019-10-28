# coding=utf-8

import requests
from functools import partial

from PyJsEngine import PyJsEngine, get_method_name

__version__ = "1.0.191025"


class RequestsJsEngine(PyJsEngine):
    DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.75 Safari/537.36"
    DEFAULT_HEADERS = {
        "User-Agent": DEFAULT_USER_AGENT,
    }
    DEFAULT_REQUEST_TIMEOUT = 10
    DEFAULT_ENCODING = "utf-8"
    DEFAULT_GET_BYTES = False

    funcn_rget = "rget"
    funcn_rpost = "rpost"
    funcn_session = "session"
    funcn_cookies = "cookies"
    funcn_input = "input"  # TODO 输入验证码
    funcn_ok = "ok"  # TODO 标记任务完成
    funcn_err = "err"  # TODO 标记任务失败

    attrn_session = "session"
    attrn_cookies = "cookies"
    attrn_url = "url"
    attrn_headers = "headers"
    attrn_data = "data"
    attrn_timeout = "timeout"
    attrn_encoding = "encoding"
    attrn_get_bytes = "get_bytes"

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

        # Register runners for Requests and js2py
        self.register_context({
            # ---- Engine functions ----
            self.funcn_rget: partial(self.run_rfunc, funcn=self.funcn_rget),
            self.funcn_rpost: partial(self.run_rfunc, funcn=self.funcn_rpost),
            # ---- Constants ----
            "user_agent": self.DEFAULT_USER_AGENT,
        })
        self.append_prepare_script(self.PREPARE_SCRIPT_REQUESTS_JS)

        self._logger.debug(msg="RequestsJsEngine loaded. ({})".format(__version__))

    def session_get(self, url, headers=None, timeout=DEFAULT_REQUEST_TIMEOUT, encoding="utf-8",
                    get_bytes=False):
        if headers is None:
            headers = self.DEFAULT_HEADERS
        session = self._session

        # 获取页面数据
        req = session.get(url, headers=headers, timeout=timeout)
        if not get_bytes:
            req.encoding = encoding
            result = req.text
        else:
            result = req.content
        return result

    def session_post(self, url, headers=None, data=None, timeout=DEFAULT_REQUEST_TIMEOUT, encoding="utf-8",
                     get_bytes=False):
        if headers is None:
            headers = self.DEFAULT_HEADERS
        session = self._session

        # 获取页面数据
        req = session.post(url, headers=headers, data=data, timeout=timeout)
        if not get_bytes:
            req.encoding = encoding
            result = req.text
        else:
            result = req.content
        return result

    def run_rfunc(self, jskwargs, *args, funcn=None):
        logger = self._logger

        # 属性
        jargs = self.args_parser(jskwargs, {
            self.attrn_url: ('url', 's', None),
            self.attrn_data: ('data', 's', None),
            self.attrn_headers: ('headers', 's', None),
        })
        url = jargs['url']
        data = jargs['data']
        headers = jargs['headers']

        try:
            if funcn == self.funcn_rget:
                pass
            elif funcn == self.funcn_rpost:
                pass
        except Exception as e:
            self.internal_exception_handler(funcn=get_method_name(), jskwargs=jskwargs, args=args, e=e)
