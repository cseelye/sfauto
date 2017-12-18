FROM cseelye/linux-shell-nox

ARG VCS_REF=unknown
ARG BUILD_DATE=unknown
ARG VERSION=0.0
LABEL maintainer="cseelye@gmail.com" \
      name="sfauto" \
      description="Container for running sfauto scripts" \
      vcs-ref="$VCS_REF" \
      build-date="$BUILD_DATE" \
      version="$VERSION"

COPY . /sfauto
RUN pip install -U -r /sfauto/requirements.txt && \
    pip install -U -r /sfauto/requirements-test.txt
    pip install yamllint
WORKDIR /sfauto
CMD ["/bin/bash"]
