"""Resolving the four compute shapes prospects actually described.

Every one of these came up by name in customer calls, so all four are first-class
rather than one blessed path plus escape hatches:

``serverless``
    No cluster spec at all. Fastest start, and the shape Databricks is steering
    customers toward.
``existing_cluster``
    A long-lived cluster owned by someone else, typically a central data team.
``instance_pool``
    A warm pool, which is the answer to multi-minute cluster start times.
``new_cluster``
    A job cluster created per run, for full control over the runtime.
"""

from ...exceptions import SparkConfigError

SERVERLESS = "serverless"
EXISTING_CLUSTER = "existing_cluster"
INSTANCE_POOL = "instance_pool"
NEW_CLUSTER = "new_cluster"

DEFAULT_RUNTIME_VERSION = "15.4.x-scala2.12"
DEFAULT_NODE_TYPE = {
    "aws": "i3.xlarge",
    "azure": "Standard_DS3_v2",
    "gcp": "n1-standard-4",
}
ENVIRONMENT_KEY = "metaflow-spark"


class ComputeSpec:
    """A normalized description of where a Databricks job should run."""

    def __init__(self, kind, cluster_id=None, new_cluster=None, environment=None):
        self.kind = kind
        self.cluster_id = cluster_id
        self.new_cluster = new_cluster or {}
        self.environment = environment

    @property
    def is_serverless(self):
        return self.kind == SERVERLESS

    def task_fields(self):
        """The task-level keys that select this compute."""
        if self.kind == SERVERLESS:
            return {"environment_key": ENVIRONMENT_KEY}
        if self.kind == EXISTING_CLUSTER:
            return {"existing_cluster_id": self.cluster_id}
        return {"new_cluster": self.new_cluster}

    def submit_fields(self, dependencies=None):
        """The top-level submit keys this compute needs."""
        if self.kind != SERVERLESS:
            return {}
        spec: dict = {"client": "3"}
        if dependencies:
            spec["dependencies"] = list(dependencies)
        return {"environments": [{"environment_key": ENVIRONMENT_KEY, "spec": spec}]}

    def describe(self):
        if self.kind == SERVERLESS:
            return "serverless compute"
        if self.kind == EXISTING_CLUSTER:
            return "existing cluster %s" % self.cluster_id
        if self.kind == INSTANCE_POOL:
            return "instance pool %s (%s workers)" % (
                self.new_cluster.get("instance_pool_id"),
                self.new_cluster.get("num_workers"),
            )
        return "new cluster (%s, %s workers)" % (
            self.new_cluster.get("spark_version"),
            self.new_cluster.get("num_workers"),
        )


def resolve_compute(config, tags=None, cloud="aws"):
    """Turn user config into a ComputeSpec.

    Explicit `compute` config wins. Otherwise the shape is inferred from which keys are
    present, which keeps the common cases short to write.
    """
    compute = config.get("compute")
    if isinstance(compute, str):
        if compute != SERVERLESS:
            raise SparkConfigError(
                "@spark(compute=%r) is not understood. Pass 'serverless', or a dict "
                "with one of: cluster_id, instance_pool_id, node_type_id." % compute
            )
        compute = {"serverless": True}
    compute = dict(compute or {})

    # Promote the shorthand keys people reach for first.
    for key in (
        "serverless",
        "cluster_id",
        "instance_pool_id",
        "num_workers",
        "node_type_id",
    ):
        if key in config and key not in compute:
            compute[key] = config[key]

    serverless = compute.pop("serverless", None)
    cluster_id = compute.pop("cluster_id", None)
    pool_id = compute.get("instance_pool_id")

    explicit = [
        name
        for name, present in (
            (SERVERLESS, bool(serverless)),
            (EXISTING_CLUSTER, bool(cluster_id)),
            (INSTANCE_POOL, bool(pool_id)),
        )
        if present
    ]
    if len(explicit) > 1:
        raise SparkConfigError(
            "Conflicting compute configuration: %s were all specified. Choose one."
            % ", ".join(explicit)
        )

    if serverless:
        return ComputeSpec(SERVERLESS)
    if cluster_id:
        return ComputeSpec(EXISTING_CLUSTER, cluster_id=cluster_id)

    # Anything else builds a cluster spec, whether or not it comes from a pool.
    new_cluster = _build_new_cluster(config, compute, tags=tags, cloud=cloud)
    kind = INSTANCE_POOL if pool_id else NEW_CLUSTER
    if not (pool_id or compute or config.get("runtime_version")):
        # Nothing was said about compute at all. Serverless is the right default: it is
        # the cheapest to start and needs no instance-type knowledge.
        return ComputeSpec(SERVERLESS)
    return ComputeSpec(kind, new_cluster=new_cluster)


def _build_new_cluster(config, compute, tags=None, cloud="aws"):
    spec = dict(compute)
    spec.setdefault(
        "spark_version", config.get("runtime_version") or DEFAULT_RUNTIME_VERSION
    )
    spec.setdefault("num_workers", 2)

    if not spec.get("instance_pool_id"):
        spec.setdefault("node_type_id", DEFAULT_NODE_TYPE.get(cloud, "i3.xlarge"))
    else:
        # A pool already fixes the instance type; sending one is an error.
        spec.pop("node_type_id", None)

    if config.get("photon"):
        spec["runtime_engine"] = "PHOTON"

    params = config.get("spark-parameters") or {}
    if params:
        conf = dict(spec.get("spark_conf") or {})
        for key, value in params.items():
            conf.setdefault(key, str(value))
        spec["spark_conf"] = conf

    if tags:
        custom = dict(spec.get("custom_tags") or {})
        custom.update(tags)
        spec["custom_tags"] = custom

    return spec
