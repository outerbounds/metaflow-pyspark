import json
import os
import metaflow
from metaflow_extensions.netflix_ext.plugins.conda.conda import Conda
from metaflow_extensions.netflix_ext.plugins.conda.env_descr import (
    CachedEnvironmentInfo,
    EnvID,
)
from metaflow_extensions.netflix_ext.plugins.conda.remote_bootstrap import (
    my_echo_always,
)

env_to_use = os.environ["_METAFLOW_CONDA_ENV"]
my_conda = Conda(my_echo_always, datastore_type="s3", mode="remote")
resolved_env = my_conda.environment(EnvID(*json.loads(env_to_use)))
try:
    my_conda.create_for_step("hosting_environment", resolved_env, do_symlink=True)
except OSError:
    pass
