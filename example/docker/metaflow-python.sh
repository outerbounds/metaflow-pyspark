#!/bin/bash

# this script requires _METAFLOW_CONDA_ENV, METAFLOW_DATASTORE_SYSROOT_S3 and METAFLOW_CODE_URL env vars

set -ex

# remove single quotes that were added for EMR serverless
_METAFLOW_CONDA_ENV=$(echo "$_METAFLOW_CONDA_ENV" | sed "s/'//g")

# setup conda env
python3 trampoline.py

# download code
echo "METAFLOW_CODE_URL: ${METAFLOW_CODE_URL}"
aws s3 cp $METAFLOW_CODE_URL .

# unpack code
code_tarball=`basename $METAFLOW_CODE_URL`
echo "code_tarball: ${code_tarball}"
tar zxvf $code_tarball

# launch the "real" python
conda_python=$(readlink ./__conda_python)
$conda_python "$@"
