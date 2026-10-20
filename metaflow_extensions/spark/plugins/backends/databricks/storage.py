"""Reading and writing the paths a Databricks job needs.

Unity Catalog Volumes are the default staging location rather than DBFS: DBFS root is
discouraged in UC-enabled workspaces, and a Volume keeps the code package and any
outputs inside the governance boundary the customer already set up.
"""

import io
import os
import posixpath

from ...exceptions import SparkConfigError, SparkException

VOLUME_PREFIX = "/Volumes/"


def is_volume_path(path):
    return bool(path) and path.startswith(VOLUME_PREFIX)


def is_cloud_uri(path):
    return bool(path) and "://" in path


def normalize_volume(path, what="volume"):
    # The workspace UI and the dbutils APIs both hand out `dbfs:/Volumes/...`, so accept
    # that spelling rather than making people strip a prefix they did not add.
    if isinstance(path, str) and path.startswith("dbfs:/Volumes/"):
        path = path[len("dbfs:") :]
    if not is_volume_path(path):
        raise SparkConfigError(
            "The %s must be a Unity Catalog Volume path starting with /Volumes/, got "
            "%r.\nFor example: /Volumes/main/metaflow/staging" % (what, path)
        )
    return path.rstrip("/")


class DatabricksStorage:
    def __init__(self, client):
        self.client = client

    # ------------------------------------------------------------------
    def upload(self, path, blob):
        """Upload bytes to a Volume path, creating parent directories as needed."""
        if not is_volume_path(path):
            raise SparkConfigError(
                "Can only upload to Unity Catalog Volume paths, got %r." % path
            )
        self.makedirs(posixpath.dirname(path))
        try:
            self.client.sdk.files.upload(path, io.BytesIO(blob), overwrite=True)
        except Exception as exc:
            raise SparkException(
                "Failed to upload to %s: %s\n"
                "Check that the Volume exists and that you have WRITE VOLUME on it."
                % (path, exc)
            ) from exc
        return path

    def makedirs(self, path):
        if not is_volume_path(path):
            return
        try:
            self.client.sdk.files.create_directory(path)
        except Exception:
            # Already existing, or the SDK version predates the method. An upload
            # failure downstream will report the real problem with better context.
            pass

    def read_text(self, path, max_bytes=None):
        try:
            response = self.client.sdk.files.download(path)
        except Exception:
            return None
        stream = getattr(response, "contents", response)
        try:
            data = stream.read() if max_bytes is None else stream.read(max_bytes)
        finally:
            close = getattr(stream, "close", None)
            if close:
                try:
                    close()
                except Exception:
                    pass
        if isinstance(data, (bytes, bytearray)):
            return data.decode("utf-8", errors="replace")
        return data

    def list_files(self, directory, suffix=None, recursive=True):
        """List files under a Volume directory."""
        out = []
        try:
            entries = list(self.client.sdk.files.list_directory_contents(directory))
        except Exception:
            return out
        for entry in entries:
            path = getattr(entry, "path", None)
            if path is None:
                continue
            if getattr(entry, "is_directory", False):
                if recursive:
                    out.extend(self.list_files(path, suffix=suffix, recursive=True))
                continue
            if suffix and not path.endswith(suffix):
                continue
            out.append(path)
        return out

    def download_dir(self, directory, local_dir, suffix=None):
        """Download every file under a Volume directory. Returns local paths."""
        paths = self.list_files(directory, suffix=suffix)
        if not paths:
            return []
        local_paths = []
        os.makedirs(local_dir, exist_ok=True)
        for remote in paths:
            rel = posixpath.relpath(remote, directory)
            local = os.path.join(local_dir, rel.replace("/", os.sep))
            os.makedirs(os.path.dirname(local), exist_ok=True)
            response = self.client.sdk.files.download(remote)
            stream = getattr(response, "contents", response)
            with open(local, "wb") as handle:
                while True:
                    chunk = stream.read(1024 * 1024)
                    if not chunk:
                        break
                    handle.write(chunk)
            local_paths.append(local)
        return local_paths

    def delete(self, path):
        try:
            self.client.sdk.files.delete(path)
        except Exception:
            pass
