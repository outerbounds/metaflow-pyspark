"""Code packaging for submit-style backends.

Closes the "single self-contained module" limitation. The rule is deliberately
predictable rather than clever: package the top-level package containing the job
function, plus anything named in ``include``. Predictable beats magic here, because the
failure mode of a guess is an ImportError inside a Spark driver, which is an expensive
place to debug.
"""

import io
import os
import sys
import tarfile

from .exceptions import SparkConfigError

EXCLUDE_DIRS = {
    "__pycache__",
    ".git",
    ".hg",
    ".svn",
    ".tox",
    ".venv",
    "venv",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".ipynb_checkpoints",
    "node_modules",
    ".metaflow",
}
EXCLUDE_SUFFIXES = (".pyc", ".pyo", ".so", ".dylib", ".tar.gz", ".whl")
MAX_PACKAGE_BYTES = 100 * 1024 * 1024


class CodePackage:
    def __init__(self, blob, module_name, roots, warnings=None):
        self.blob = blob
        self.module_name = module_name
        self.roots = roots
        self.warnings = warnings or []

    @property
    def size(self):
        return len(self.blob)


def python_version():
    return "%d.%d.%d" % sys.version_info[:3]


def pickle_inputs(inputs):
    """Serialize job_parameters for a remote driver."""
    import pickle

    try:
        return pickle.dumps(inputs or {}, protocol=4)
    except Exception as exc:
        raise SparkConfigError(
            "Could not pickle job_parameters for the Spark job: %s\n"
            "job_parameters have to cross a process boundary, so they must be "
            "picklable and their classes importable on the cluster. Pass plain types, "
            "or use mode='connect' where no serialization happens." % exc
        ) from exc


def read_source(path):
    with open(path, "rb") as handle:
        return handle.read()


def resolve_job_module(job_func):
    """Return (module_name, root_dir, file_path) for the module defining `job_func`."""
    import inspect

    module = inspect.getmodule(job_func)
    source = getattr(module, "__file__", None) if module else None
    if module is None or not source:
        raise SparkConfigError(
            "Could not locate the source file for the @spark job %r. Define the job "
            "function in an importable module rather than interactively."
            % getattr(job_func, "__name__", job_func)
        )

    path = os.path.abspath(source)
    name = module.__name__

    if name == "__main__":
        # The job lives in the flow file itself. Importable, but only if the cluster can
        # import whatever the flow file imports, which usually means metaflow.
        name = os.path.splitext(os.path.basename(path))[0]
        return name, os.path.dirname(path), path

    package = getattr(module, "__package__", None) or ""
    if not package:
        return name, os.path.dirname(path), path

    # Walk up one directory per package level so the whole top-level package ships and
    # relative imports inside it keep working.
    depth = len(package.split("."))
    root = os.path.dirname(path)
    for _ in range(depth):
        root = os.path.dirname(root)
    return name, root, path


def build_package(job_func, include=None, extra_modules=None):
    """Build a tar.gz containing the job's code.

    `include` accepts files or directories; each is added at its basename so that the
    extracted root is importable.
    """
    module_name, root, job_path = resolve_job_module(job_func)
    warnings = []

    entries = []
    if os.path.isdir(root) and _is_package_dir(job_path):
        # The job is inside a package: ship the package directory itself.
        pkg_dir = _top_level_package_dir(job_path)
        entries.append((pkg_dir, os.path.basename(pkg_dir)))
    else:
        entries.append((job_path, os.path.basename(job_path)))
        if module_name == os.path.splitext(os.path.basename(job_path))[
            0
        ] and _defines_flow(job_path):
            warnings.append(
                "The @spark job is defined in the flow file, so the cluster has to be "
                "able to import it, which usually means metaflow must be installed "
                "there. Moving the job into its own module avoids that."
            )

    for path in include or []:
        abs_path = os.path.abspath(path)
        if not os.path.exists(abs_path):
            raise SparkConfigError("@spark(include=...) path does not exist: %s" % path)
        entries.append((abs_path, os.path.basename(abs_path.rstrip(os.sep))))

    for module in extra_modules or []:
        resolved = _module_path(module)
        if resolved is None:
            warnings.append("Could not locate module '%s' to package." % module)
        else:
            entries.append((resolved, os.path.basename(resolved)))

    buf = io.BytesIO()
    seen = set()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for source, arcname in entries:
            if arcname in seen:
                continue
            seen.add(arcname)
            if os.path.isdir(source):
                _add_dir(tar, source, arcname)
            else:
                tar.add(source, arcname=arcname)

    blob = buf.getvalue()
    if len(blob) > MAX_PACKAGE_BYTES:
        raise SparkConfigError(
            "The @spark code package is %.1fMB, over the %dMB limit. Narrow it with "
            "@spark(include=[...]) rather than packaging a whole project tree."
            % (len(blob) / 1024**2, MAX_PACKAGE_BYTES // 1024**2)
        )
    return CodePackage(blob, module_name, [e[1] for e in entries], warnings)


def _add_dir(tar, source, arcname):
    for dirpath, dirnames, filenames in os.walk(source):
        dirnames[:] = [d for d in dirnames if d not in EXCLUDE_DIRS]
        for filename in filenames:
            if filename.endswith(EXCLUDE_SUFFIXES):
                continue
            full = os.path.join(dirpath, filename)
            rel = os.path.relpath(full, source)
            tar.add(full, arcname=os.path.join(arcname, rel))


def _is_package_dir(job_path):
    return os.path.exists(os.path.join(os.path.dirname(job_path), "__init__.py"))


def _top_level_package_dir(job_path):
    """Walk up while each directory is a package, and return the outermost one."""
    current = os.path.dirname(job_path)
    while os.path.exists(os.path.join(os.path.dirname(current), "__init__.py")):
        current = os.path.dirname(current)
    return current


def _defines_flow(path):
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as handle:
            return "FlowSpec" in handle.read()
    except OSError:
        return False


def _module_path(name):
    module = sys.modules.get(name)
    path = getattr(module, "__file__", None) if module else None
    if path is None:
        try:
            import importlib.util

            spec = importlib.util.find_spec(name)
            path = spec.origin if spec else None
        except Exception:
            return None
    if not path:
        return None
    path = os.path.abspath(path)
    if os.path.basename(path) == "__init__.py":
        return os.path.dirname(path)
    return path
