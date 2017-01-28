FROM ubuntu:16.04

RUN apt-get update && \
    apt-get --assume-yes dist-upgrade && \
    DEBIAN_FRONTEND=noninteractive apt-get install --assume-yes \
        pkg-config \
        curl \
        build-essential \
        ipmitool \
        libvirt-bin \
        libvirt-dev \
        python2.7 \
        python2.7-dev && \
    curl https://bootstrap.pypa.io/get-pip.py | python2.7 && \
    apt-get autoremove && \
    apt-get clean && \
    rm --force --recursive /var/lib/apt/lists/* /tmp/* /var/tmp/*
COPY requirements.txt requirements.txt
RUN pip install -U -r requirements.txt && \
    rm -f requirements.txt

