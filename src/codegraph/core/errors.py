class CodegraphError(Exception):
    pass


class InvariantError(CodegraphError):
    pass


class ServiceDegraded(CodegraphError):
    def __init__(self, service: str, reason: str):
        self.service = service
        self.reason = reason
        super().__init__(f"service {service!r} degraded: {reason}")
