ARG PYTHON_VERSION=3.8

FROM python:${PYTHON_VERSION}-slim
# For grpc https://github.com/grpc/grpc/issues/24556#issuecomment-751797589
RUN apt-get update -qqy && apt-get install -qqy python-dev build-essential git
COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY --from=google/cloud-sdk:alpine /google-cloud-sdk /google-cloud-sdk
ENV PATH /google-cloud-sdk/bin:$PATH
WORKDIR /app
COPY . .
ENTRYPOINT ["/app/bin/generate"]
