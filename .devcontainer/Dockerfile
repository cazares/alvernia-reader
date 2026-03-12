FROM mcr.microsoft.com/devcontainers/base:ubuntu-24.04

RUN apt-get update && apt-get install -y --no-install-recommends \
  ffmpeg \
  aria2 \
  git \
  curl \
  build-essential \
  cmake \
  pkg-config \
  && rm -rf /var/lib/apt/lists/*
