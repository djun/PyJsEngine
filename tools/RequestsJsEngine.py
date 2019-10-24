# coding=utf-8

import requests
from functools import partial

from PyJsEngine import PyJsEngine

__version__ = "1.0.191024"


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

    attrn_session = "session"
    attrn_cookies = "cookies"
    attrn_url = "url"
    attrn_headers = "headers"
    attrn_data = "data"
    attrn_timeout = "timeout"
    attrn_encoding = "encoding"
    attrn_get_bytes = "get_bytes"

    PREPARE_SCRIPT_REQUESTS_JS = r"""
        function Gather_data(src, fields) {
            var data = {};
            for (var i in fields) {
                var key = fields[i];
                data[key] = src[key];
            }
            return data;
        }
        
        function Get_(url) {
            return Rget({
                url: url,
            });  // TODO
        }
        
        function Post_(url, data=null) {
            return Rget({
                url: url,
                data: JSON.stringnify(data),
            });  // TODO
        }
    """

    def __init__(self, logger=None, msg_handler=None, **kwargs):
        super().__init__(logger=logger, msg_handler=msg_handler, **kwargs)

        self._session = requests.session()
        self._cookies = requests.cookies.RequestsCookieJar()
        self._session.cookies = self._cookies

        # Register runners for Requests and js2py
        self.register_context({
            self.funcn_rget: partial(self.run_rfunc, funcn=self.funcn_rget),
            self.funcn_rpost: partial(self.run_rfunc, funcn=self.funcn_rpost),
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
            self.attrn_encoding: ('encoding', 's', None),
            self.attrn_get_bytes: ('get_bytes', 'b', False),
        })
        url = jargs['url']
        data = jargs['data']
        encoding = jargs['encoding'] or self._encoding
        get_bytes = jargs['get_bytes']
