# Built with repo root as build context (see docker-compose.yml) so this
# can COPY both the shared common/ package and streaming/ itself.
FROM apache/spark-py:v3.4.0

USER root

COPY common /opt/pulseguard/common
COPY streaming /opt/pulseguard/streaming

# requirements.txt's "-e ../common" line is written for a host-machine venv
# run from inside streaming/ (see README); older pip inside this base image
# resolves relative editable paths against the cwd, not the requirements
# file, so common/ is installed explicitly by absolute path here instead.
RUN pip install --no-cache-dir pyarrow==17.0.0 pandas==2.2.2 redis==5.0.8 \
    && pip install --no-cache-dir /opt/pulseguard/common \
    && pip install --no-cache-dir /opt/pulseguard/streaming

WORKDIR /opt/pulseguard/streaming
