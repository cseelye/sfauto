FROM ubuntu:16.04

ENV TERM=xterm-256color
RUN apt-get update && \
    apt-get --assume-yes dist-upgrade && \
    DEBIAN_FRONTEND=noninteractive apt-get install --assume-yes \
        build-essential \
        curl \
        ipmitool \
        jq \
        libvirt-bin \
        libvirt-dev \
        net-tools \
        nfs-common \
        pkg-config \
        pigz \
        python2.7 \
        python2.7-dev \
        rsync \
        sshpass \
        sysstat \
        vim \
        wget && \
    curl https://bootstrap.pypa.io/get-pip.py | python2.7 && \
    apt-get autoremove && \
    apt-get clean && \
    rm --force --recursive /var/lib/apt/lists/* /tmp/* /var/tmp/*
COPY requirements.txt requirements.txt
RUN pip install -U -r requirements.txt && \
    rm -f requirements.txt

