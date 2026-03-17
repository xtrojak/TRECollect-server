FROM python:3.11-slim

# Install cron and other dependencies
RUN apt-get update && \
    apt-get install -y cron && \
    rm -rf /var/lib/apt/lists/*

# Create working directories
WORKDIR /app
RUN mkdir /app/logs

# Copy source code
COPY . /app

# Create virtual environment and install dependencies
RUN python -m venv TRECollect-server && \
    . TRECollect-server/bin/activate && \
    pip install --upgrade pip && \
    if [ -f requirements.txt ]; then pip install -r requirements.txt; fi

# Add cron jobs: processing every minute, statistics every 5 minutes
RUN echo "* * * * * cd /app && flock -n /tmp/processing_script.lock /bin/bash /app/processing_script.sh" > /etc/cron.d/mycron && \
    echo "*/5 * * * * cd /app && flock -n /tmp/statistics_script.lock /bin/bash /app/statistics_script.sh" >> /etc/cron.d/mycron && \
    echo "0 6 * * * cd /app && flock -n /tmp/backup_script.lock /bin/bash /app/backup_script.sh" >> /etc/cron.d/mycron && \
    chmod 0644 /etc/cron.d/mycron && \
    crontab /etc/cron.d/mycron

# Make log folder available outside container
VOLUME ["/app/logs"]
VOLUME ["/app/statistics"]
VOLUME ["/app/timestamps"]
VOLUME ["/export"]

# Copy status page to export, then start cron
CMD ["sh", "-c", "mkdir -p /export && cp /app/TRECollect-status.html /export/ && exec cron -f"]
