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
RUN python -m venv TRECollect-logsheets && \
    . TRECollect-logsheets/bin/activate && \
    pip install --upgrade pip && \
    if [ -f requirements.txt ]; then pip install -r requirements.txt; fi

# Add cron job to run script every minute and log output
RUN echo "* * * * * cd /app && flock -n /tmp/processing_script.lock /bin/bash /app/processing_script.sh" > /etc/cron.d/mycron && \
    chmod 0644 /etc/cron.d/mycron && \
    crontab /etc/cron.d/mycron

# Make log folder available outside container
VOLUME ["/app/logs"]

# Start cron in foreground
CMD ["cron", "-f"]
