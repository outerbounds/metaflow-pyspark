"""End-to-end exercise of @spark with a stand-in session backend.

A fake backend rather than a real cluster tests the part that is easy to get wrong and
hard to notice: the decorator's interaction with Metaflow's task lifecycle. A real
SparkSession is not picklable, so if the session were still attached to the flow when
Metaflow persists artifacts, every step would fail.
"""

from contextlib import contextmanager

from metaflow import FlowSpec, current, spark, step

from metaflow_extensions.spark.plugins.backends import SessionBackend, register_backend


class FakeSession:
    """Unpicklable on purpose, exactly like a real SparkSession."""

    version = "3.5.0-fake"

    def __init__(self, tag):
        self.tags = [tag]

    def __reduce__(self):
        raise TypeError("cannot pickle a SparkSession")


class FakeSessionBackend(SessionBackend):
    name = "fake-session"

    @contextmanager
    def session(self, ctx):
        yield FakeSession("metaflow-%s" % ctx.pathspec.replace("/", "-"))


register_backend("fake-session", FakeSessionBackend)


class SessionFlow(FlowSpec):
    @spark(backend="fake-session")
    @step
    def start(self):
        assert current.spark is self.spark, "current.spark and self.spark must agree"
        self.version = self.spark.version
        self.tags = list(self.spark.tags)
        self.next(self.end)

    @step
    def end(self):
        # Reaching here at all proves the session was detached before persistence.
        assert self.version == "3.5.0-fake"
        assert not hasattr(self, "spark")
        assert self.tags[0].startswith(
            "metaflow-SessionFlow-%s-start-" % current.run_id
        )
        print("session flow ok")


if __name__ == "__main__":
    SessionFlow()
