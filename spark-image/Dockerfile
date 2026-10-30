# Image for the driver and executor pods of generated pipelines.
#
# The official apache/spark:3.5.0 image ships with Python but without numpy,
# and pyspark.ml cannot be imported without it, so every MLlib program would
# fail in the pod with "No module named 'numpy'". This image adds just that.
#
# Build it once on the machine that runs Docker Desktop's Kubernetes (the
# cluster uses the local Docker images, so no registry is needed):
#   docker build -t spark-ml:3.5.0 ./spark-image
FROM apache/spark:3.5.0

USER root
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3-pip && \
    rm -rf /var/lib/apt/lists/* && \
    python3 -m pip install --no-cache-dir numpy==1.24.4

# The pods keep running as root (the base image would switch to the user
# 'spark', uid 185). The shared folder is a Windows folder, which Docker Desktop
# exposes to the Kubernetes node owned by root: uid 185 can read the programs and
# data there but cannot write the result preview or an output path, and fails
# with 'PermissionError: [Errno 13]'. Acceptable for the local test cluster; on a
# real cluster the hostPath folder should instead be made writable for uid 185.
