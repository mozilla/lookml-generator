FROM python:3.8-slim
MAINTAINER Frank Bertsch <frank@mozilla.com>

ENV USER_ID="10001"
ENV GROUP_ID="app"
ENV HOME="/app"

RUN groupadd --gid ${USER_ID} ${GROUP_ID} && \
    useradd --create-home --uid ${USER_ID} --gid ${GROUP_ID} --home-dir /app ${GROUP_ID}

# For grpc https://github.com/grpc/grpc/issues/24556#issuecomment-751797589
RUN apt-get update -qqy && \
    apt-get install -qqy python-dev build-essential git curl software-properties-common

RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-key C99B11DEB97541F0
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-key C99B11DEB97541F0
RUN apt-add-repository https://cli.github.com/packages
RUN apt update
RUN apt install -y gh

COPY --from=google/cloud-sdk:alpine /google-cloud-sdk /google-cloud-sdk
ENV PATH /google-cloud-sdk/bin:$PATH

WORKDIR ${HOME}
RUN chown -R ${USER_ID}:${GROUP_ID} ${HOME}

RUN pip install --upgrade pip
COPY requirements.txt ${HOME}
RUN chown ${USER_ID}:${GROUP_ID} ${HOME}/requirements.txt
RUN pip install -r ${HOME}/requirements.txt

COPY . ${HOME}/lookml-generator
RUN chown -R ${USER_ID}:${GROUP_ID} ${HOME}/lookml-generator
ENV PATH $PATH:${HOME}/lookml-generator/bin
RUN pip install --no-dependencies -e ${HOME}/lookml-generator

USER ${USER_ID}

ENTRYPOINT ["generate"]
