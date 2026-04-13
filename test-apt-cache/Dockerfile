FROM ubuntu:22.04

# Point APT at the local Depot cache repositories.
RUN echo 'deb http://depot/repository/ubuntu-jammy jammy main restricted universe multiverse' > /etc/apt/sources.list && \
    echo 'deb http://depot/repository/ubuntu-jammy-updates jammy-updates main restricted universe multiverse' >> /etc/apt/sources.list && \
    echo 'deb http://depot/repository/ubuntu-jammy-security jammy-security main restricted universe multiverse' >> /etc/apt/sources.list

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        build-essential \
        curl \
        git \
        vim \
        less \
        jq \
        ca-certificates \
        python3 \
        python3-pip \
    && rm -rf /var/lib/apt/lists/*

CMD ["/bin/bash"]
