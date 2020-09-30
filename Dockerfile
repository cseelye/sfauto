ARG SFAUTO_PYTHON=3.8
FROM python:${SFAUTO_PYTHON}-buster

ARG VCS_REF=unknown
ARG BUILD_DATE=unknown
ARG VERSION=0.0
LABEL maintainer="cseelye@gmail.com" \
      org.opencontainers.image.authors="cseelye@gmail.com" \
      org.opencontainers.image.title="sfauto" \
      org.opencontainers.image.description="Container for running sfauto scripts" \
      org.opencontainers.image.url="https://github.com/cseelye/sfauto" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.version="${VERSION}"

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install --assume-yes \
        ack \
        ipmitool \
        libvirt-clients \
        libvirt-dev \
        smbclient \
        pkg-config \
        vim \
        && \
    apt-get autoremove --assume-yes && \
    apt-get clean && \
    rm --force --recursive /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY requirements.txt requirements-test.txt /tmp/
RUN pip install -U pip && \
    pip install -U -r /tmp/requirements.txt && \
    pip install -U -r /tmp/requirements-test.txt && \
    rm -f /tmp/requirements.txt /tmp/requirements-test.txt

COPY . /sfauto
WORKDIR /sfauto
CMD ["/bin/bash"]
