FROM cseelye/linux-shell

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

COPY . /sfauto
RUN pip install -U pip && \
    pip install -U -r /sfauto/requirements.txt && \
    pip install -U -r /sfauto/requirements-test.txt \
    pip install yamllint
WORKDIR /sfauto
CMD ["/bin/bash"]
