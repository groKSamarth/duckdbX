# DuckDBX Docker image - AWS-optimized for Lambda/Fargate
FROM amazonlinux:2023

# Install Python 3.11 and pip
RUN dnf install -y python3.11 python3.11-pip && \
    dnf clean all && \
    rm -rf /var/cache/dnf

# Create symlink for python
RUN ln -s /usr/bin/python3.11 /usr/bin/python && \
    ln -s /usr/bin/pip3.11 /usr/bin/pip

# Set working directory
WORKDIR /app

# Copy constraint and requirements files
# Note: Build context should be from project root: docker build -f docker/Dockerfile .
COPY docker/requirements.txt docker/constraints.txt ./

# Install Python dependencies with constraints
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir --constraint constraints.txt -r requirements.txt

# Install DuckDB Delta Sharing extension (for future use)
# This will be enabled when we add Databricks connection support
# Note: delta_sharing extension may not be available in all DuckDB versions
RUN python -c "import duckdb; conn = duckdb.connect(); \
    try: \
        conn.execute('INSTALL delta_sharing;'); \
        conn.execute('LOAD delta_sharing;'); \
        print('Delta Sharing extension installed'); \
    except Exception as e: \
        print(f'Delta Sharing extension not available: {e}'); \
        pass"

# Expose DuckDB HTTP interface port (default: 3141)
EXPOSE 3141

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV DUCKDB_LISTEN_HOST=0.0.0.0
ENV DUCKDB_LISTEN_PORT=3141

# Start DuckDB in server mode
# For now, we'll keep it simple - in production this would run DuckDB HTTP server
CMD ["python", "-c", "import duckdb; import time; conn = duckdb.connect(); print('DuckDB ready'); time.sleep(3600)"]

