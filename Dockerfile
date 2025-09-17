# Dockerfile for HyDocPusher
# Version: 1.0.0
# Python Version: 3.9.6

# Build stage
FROM harbor.trscd.com.cn/baseapp/python:3.9.23-slim-bullseye as builder

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN sed -i 's/deb.debian.org/mirrors.ustc.edu.cn/g' /etc/apt/sources.list \
    && sed -i -e 's/^APT/# APT/' -e 's/^DPkg/# DPkg/' /etc/apt/apt.conf.d/docker-clean \
    && apt-get update && apt-get install -y gcc g++ libpq-dev \
    && apt-get clean \
    && rm -rf /var/cache/apt/*  /var/lib/apt/lists/*

# Create virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --upgrade pip \
    && pip install -r requirements.txt

# Production stage
FROM harbor.trscd.com.cn/baseapp/python:3.9.23-slim-bullseye

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH" \
    DEBIAN_FRONTEND=noninteractive

# Install runtime dependencies

RUN sed -i 's/deb.debian.org/mirrors.ustc.edu.cn/g' /etc/apt/sources.list \
    && sed -i -e 's/^APT/# APT/' -e 's/^DPkg/# DPkg/' /etc/apt/apt.conf.d/docker-clean \
    && apt-get update && apt-get install -y curl libpq5 vim wget net-tools tzdata bash \
    && apt-get clean \
    && rm -rf /var/cache/apt/*  /var/lib/apt/lists/*

# Create app user
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv

# Create app directories
RUN mkdir -p /app /app/config /app/logs /app/scripts \
    && chown -R appuser:appuser /app

# Set working directory
WORKDIR /app

# Copy application code
COPY --chown=appuser:appuser . .

# Copy entrypoint script
#COPY --chown=appuser:appuser docker/entrypoint.sh /entrypoint.sh
# RUN chmod +x /entrypoint.sh

# Create logs directory
RUN mkdir -p /app/logs && chown appuser:appuser /app/logs

# Switch to app user
USER appuser

# Expose port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Set entrypoint
# ENTRYPOINT ["/entrypoint.sh"]

# Default command
CMD ["python", "main.py"]
