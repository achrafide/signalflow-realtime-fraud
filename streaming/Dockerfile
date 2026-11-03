FROM apache/spark:3.5.1-scala2.12-java17-python3-ubuntu
USER root
RUN mkdir -p /home/spark/.ivy2/cache && \
    chown -R spark:spark /home/spark/.ivy2 && \
    chmod -R 755 /home/spark/.ivy2 && \
    pip install --no-cache-dir \
      redis==5.0.4 pyarrow==16.1.0 \
      xgboost==2.0.3 joblib pyyaml pandas
# do NOT pip-install pyspark: the base image bundles 3.5.1; 3.5.5 would shadow it
USER spark