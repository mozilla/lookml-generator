FROM python:3.8.9-slim

LABEL maintainer="kignasiak@mozilla.com"

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

COPY --from=google/cloud-sdk:339.0.0-alpine /google-cloud-sdk /google-cloud-sdk
ENV PATH /google-cloud-sdk/bin:$PATH

WORKDIR ${HOME}

COPY requirements.txt .

RUN pip install --upgrade pip \
    && pip install --no-deps --no-cache-dir -r requirements.txt \
    && rm requirements.txt

COPY . ./lookml-generator
RUN pip install --no-dependencies --no-cache-dir -e ./lookml-generator
ENV PATH $PATH:${HOME}/lookml-generator/bin

RUN chown -R ${USER_ID}:${GROUP_ID} ${HOME}
USER ${USER_ID}

ENTRYPOINT ["generate"]
