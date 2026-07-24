"""Hand-rolled HTTP client SDK convention (GAPS §2/pilot gap 1): the route lives
in a `@path_template(...)` decorator, the verb in a `Method.X` enum-like
attribute passed to `Request(...)`, and the actual network call is a level of
indirection away (`self.driver.fetch_content(request)`) -- see
app/clients/doc_client.py, the class that actually uses this convention."""


class Method:
    GET = "GET"
    POST = "POST"


class Request:
    def __init__(self, method: str, host: str, doc_uid: str) -> None:
        self.method = method
        self.host = host
        self.doc_uid = doc_uid


class ProxyRequest(Request):
    """Proxied Request subclass (M7 T5, pilot-rerun verb_unresolved=15): real SDKs
    substitute `ProxyRequest(Method.X, ...)` for `Request(...)` in some methods --
    a `request_ctor: "Request"` idiom alone never matches this ctor's own name, so
    HttpVerbFromSpec.request_ctor accepts "|"-separated alternatives
    ("Request|ProxyRequest", see workspace.yaml's status-client-proxy-sdk)."""


class Driver:
    async def fetch_content(self, request: Request) -> dict:
        return {"doc_uid": request.doc_uid}


def path_template(template: str):
    def _decorate(fn):
        fn.__path_template__ = template
        return fn

    return _decorate


class BaseClient:
    def __init__(self, host: str) -> None:
        self.host = host
        self.driver = Driver()
