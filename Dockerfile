FROM cseelye/linux-shell

COPY . /sfauto
RUN pip install -U -r /sfauto/requirements.txt && \
    pip install -U -r /sfauto/requirements-test.txt
WORKDIR /sfauto
CMD ["/bin/bash"]
