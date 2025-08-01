FROM mcr.microsoft.com/vscode/devcontainers/python:3.13
COPY --from=ghcr.io/astral-sh/uv:0.8.4 /uv /uvx /bin/
ARG USERNAME=vscode

RUN apt-get update && \
  apt-get install -y --no-install-recommends libgdal-dev python3-dev openjdk-17-jdk && \
  rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install pipx --no-cache-dir
RUN python3 -m pipx ensurepath
RUN pipx install rust-just

RUN echo 'export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))' >> /etc/profile.d/java.sh && \
  echo 'export PATH=$JAVA_HOME/bin:$PATH' >> /etc/profile.d/java.sh

WORKDIR /app

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1

# Copy from the cache instead of linking since it's a mounted volume
ENV UV_LINK_MODE=copy

COPY . /app

# Install the project's dependencies using the lockfile and settings
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv venv && just sync duckdb

ENV VENV_DIR=.venv
RUN chown -R $USERNAME $VENV_DIR && chmod -R 755 $VENV_DIR

ENV IBIS_PROJECT=.
RUN chown -R $USERNAME $IBIS_PROJECT

# Place executables in the environment at the front of the path
ENV PATH="/app/.venv/bin:$PATH"

SHELL ["/bin/bash", "-c", "source .venv/bin/activate"]

ENTRYPOINT []

USER $USERNAME
