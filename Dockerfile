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

# Fail the build, not the DAGs, when the GRIB stack is incoherent (mismatched eccodes
# library/definitions, or a driver leaking ECCODES_DEFINITION_PATH process-wide).
RUN micromamba run -n base python -m tethys_tasks.check_grib_stack

# DWD's boot_extra.def compares its version string to the eccodes library's exactly, so
# eccodes-cosmo-resources 2.47.0.1 against libeccodes 2.47.3 warns on every GRIB open even
# though meteodata-lab declares both 2.47.* ranges compatible (decoding is bit-identical).
# Set below the check above on purpose: build logs still show a real skew, DAG logs stay clean.
ENV ECCODES_VERSION_CHECK_OFF=1

# Set environment variables if needed
# ENV PYTHONPATH=/app

# Default command to run when the container starts
# This can be overridden by Airflow's DockerOperator
ENTRYPOINT ["micromamba", "run", "-n", "base", "python", "main.py"]
