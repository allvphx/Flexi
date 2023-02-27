FROM alpine
RUN apk add --update make cmake gcc g++ gfortran
RUN apk add --update python3 py3-pip python3-dev
RUN python3 -m venv /venv && \
    source /venv/bin/activate && \
    pip3 install numpy grpcio grpcio-tools && \
    pip3 install --upgrade google-api-python-client
WORKDIR /app
COPY . .
