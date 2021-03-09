FROM python:3.8-slim AS build-image
RUN apt update && apt install -y --no-install-recommends gcc build-essential git libffi-dev libssl-dev libleveldb-dev libxslt1-dev libc-ares-dev libsnappy-dev 
COPY . /tmp
RUN /tmp/docker/setup-build.sh

FROM python:3.8-slim AS main-image
COPY --from=build-image /opt/minemeld /opt/minemeld
RUN /opt/minemeld/setup-main.sh
EXPOSE 5000
ENTRYPOINT ["/opt/minemeld/docker-entrypoint.sh"]
CMD ["minemeld"]