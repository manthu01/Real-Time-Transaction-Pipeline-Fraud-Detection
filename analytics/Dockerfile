# Built with repo root as build context (see docker-compose.yml). Shares
# the streaming job's base image and Spark version so the same Delta Lake
# JARs work for both the continuous bronze write and this batch reader.
FROM apache/spark-py:v3.4.0

USER root

COPY common /opt/pulseguard/common
COPY streaming /opt/pulseguard/streaming
COPY detection /opt/pulseguard/detection
COPY analytics /opt/pulseguard/analytics

# scikit-learn + joblib: the retrain-check's Isolation Forest (shadow)
# challenger. lightgbm: its LightGBM (champion) challenger -- needed since
# LightGBM became the model that decides alerts; pinned to the version that
# trains and serves it (detection/requirements.txt, streaming/Dockerfile).
RUN pip install --no-cache-dir pyarrow==17.0.0 pandas==2.2.2 scikit-learn==1.5.2 joblib==1.4.2 lightgbm==4.7.0 \
    && pip install --no-cache-dir /opt/pulseguard/common \
    && pip install --no-cache-dir /opt/pulseguard/streaming \
    && pip install --no-cache-dir /opt/pulseguard/detection \
    && pip install --no-cache-dir /opt/pulseguard/analytics

WORKDIR /opt/pulseguard/analytics
