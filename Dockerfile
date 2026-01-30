# Use micromamba for faster environment creation
FROM mambaorg/micromamba:latest

# Copy environment.yml to the container
COPY --chown=$MAMBA_USER:$MAMBA_USER environment.yml /tmp/environment.yml

# Install dependencies
RUN micromamba install -y -n base -f /tmp/environment.yml && \
    micromamba clean --all --yes

# COPY meteoraster /tmp/meteoraster
# RUN micromamba run -n base pip install -e /tmp/meteoraster

# Set working directory
WORKDIR /app

# Copy the rest of the application code
COPY --chown=$MAMBA_USER:$MAMBA_USER . .

# Set environment variables if needed
# ENV PYTHONPATH=/app

# Default command to run when the container starts
# This can be overridden by Airflow's DockerOperator
ENTRYPOINT ["micromamba", "run", "-n", "base", "python", "main.py"]
