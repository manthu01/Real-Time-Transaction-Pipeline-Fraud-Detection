# Built with repo root as build context (see docker-compose.yml) so this
# can COPY the shared common/ package, streaming/ itself, and the
# detection package (rule engine + scoring -- not the training script,
# which needs matplotlib this image doesn't carry).
FROM apache/spark-py:v3.4.0

USER root

COPY common /opt/pulseguard/common
COPY streaming /opt/pulseguard/streaming
COPY detection /opt/pulseguard/detection

# requirements.txt's "-e ../common" line is written for a host-machine venv
# run from inside streaming/ (see README); older pip inside this base image
# resolves relative editable paths against the cwd, not the requirements
# file, so common/ is installed explicitly by absolute path here instead.
# scikit-learn + joblib are runtime deps for unpickling the trained
# Isolation Forest (shadow scorer), not for training it. lightgbm loads
# the champion model and computes per-alert TreeSHAP (pred_contrib); it's
# pinned to the exact version that trains lightgbm.txt (see
# detection/requirements.txt). Its PyPI wheels cover both x86_64
# (manylinux_2_28, needs glibc >= 2.28 -- this Ubuntu 22.04 base has 2.35)
# and aarch64 (manylinux2014), and the base image is published for both
# amd64 and arm64, so neither arch falls back to a source build. Its
# OpenMP runtime (libgomp1) already ships in the base image on both
# arches; it is deliberately NOT apt-installed here -- doing so upgrades
# libc-bin, whose ldconfig post-install step segfaults under QEMU, which
# breaks cross-building the arm64 image from an x86 machine. The final
# `import lightgbm` fails the build instead if the native library ever
# can't load.
RUN pip install --no-cache-dir pyarrow==17.0.0 pandas==2.2.2 redis==5.0.8 scikit-learn==1.5.2 joblib==1.4.2 lightgbm==4.7.0 \
    && pip install --no-cache-dir /opt/pulseguard/common \
    && pip install --no-cache-dir /opt/pulseguard/streaming \
    && pip install --no-cache-dir /opt/pulseguard/detection \
    && python3 -c "import lightgbm; print('lightgbm', lightgbm.__version__, 'loads')"

WORKDIR /opt/pulseguard/streaming
