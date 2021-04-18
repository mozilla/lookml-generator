FROM python:3.8-slim
MAINTAINER Frank Bertsch <frank@mozilla.com>

ARG USER_ID="10001"
ARG GROUP_ID="app"
ARG HOME="/app"

ENV HOME=${HOME}
RUN groupadd --gid ${USER_ID} ${GROUP_ID} && \
    useradd --create-home --uid ${USER_ID} --gid ${GROUP_ID} --home-dir /app ${GROUP_ID}

# For grpc https://github.com/grpc/grpc/issues/24556#issuecomment-751797589
RUN apt-get update -qqy && \
    apt-get install -qqy python-dev build-essential git

COPY --from=google/cloud-sdk:alpine /google-cloud-sdk /google-cloud-sdk
ENV PATH /google-cloud-sdk/bin:$PATH

WORKDIR ${HOME}
RUN chown -R ${USER_ID}:${GROUP_ID} ${HOME}

RUN pip install --upgrade pip
COPY --chown=${USER_ID}:${GROUP_ID} requirements.txt ${HOME}
RUN pip install -r ${HOME}/requirements.txt

COPY --chown=${USER_ID}:${GROUP_ID} . ${HOME}/lookml-generator
ENV PATH $PATH:${HOME}/lookml-generator/bin
RUN pip install --no-dependencies -e ${HOME}/lookml-generator

USER ${USER_ID}

ENTRYPOINT ["generate"]
