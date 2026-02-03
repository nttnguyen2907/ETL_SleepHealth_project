FROM apache/airflow:2.7.1-python3.11

USER root:root
COPY requirements.txt /tmp/requirements.txt
RUN python -m pip install --no-cache-dir -r /tmp/requirements.txt

# Ensure 'airflow' user exists (some base images may omit it)
RUN id -u airflow >/dev/null 2>&1 || (groupadd --system airflow && useradd --system --create-home --gid airflow airflow)
RUN chown -R airflow:airflow /home/airflow || true

USER airflow
