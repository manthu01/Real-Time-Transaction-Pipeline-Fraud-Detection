# Built with repo root as build context (see docker-compose.yml) so this
# can COPY the shared common/ package, streaming/ itself, and the
# detection package (rule engine + scoring -- not the training script,
# which needs lightgbm/matplotlib this image doesn't carry).
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
# Isolation Forest at scoring time, not for training it.
RUN pip install --no-cache-dir pyarrow==17.0.0 pandas==2.2.2 redis==5.0.8 scikit-learn==1.5.2 joblib==1.4.2 \
    && pip install --no-cache-dir /opt/pulseguard/common \
    && pip install --no-cache-dir /opt/pulseguard/streaming \
    && pip install --no-cache-dir /opt/pulseguard/detection

WORKDIR /opt/pulseguard/streaming
