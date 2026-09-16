# Built with repo root as build context (see docker-compose.yml). Shares
# the streaming job's base image and Spark version so the same Delta Lake
# JARs work for both the continuous bronze write and this batch reader.
FROM apache/spark-py:v3.4.0

USER root

COPY common /opt/pulseguard/common
COPY streaming /opt/pulseguard/streaming
COPY detection /opt/pulseguard/detection
COPY analytics /opt/pulseguard/analytics

# scikit-learn + joblib: the retrain-check's Isolation Forest path.
# lightgbm is deliberately NOT installed here -- pulseguard_detection.train
# lazy-imports it only inside train_lightgbm(), which this job never calls.
RUN pip install --no-cache-dir pyarrow==17.0.0 pandas==2.2.2 scikit-learn==1.5.2 joblib==1.4.2 \
    && pip install --no-cache-dir /opt/pulseguard/common \
    && pip install --no-cache-dir /opt/pulseguard/streaming \
    && pip install --no-cache-dir /opt/pulseguard/detection \
    && pip install --no-cache-dir /opt/pulseguard/analytics

WORKDIR /opt/pulseguard/analytics
