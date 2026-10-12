ARG JAVA_VERSION=11

FROM maven:3.8.8-eclipse-temurin-${JAVA_VERSION} AS build
ARG SKIP_TESTS=true
ARG HTTP_CLIENT=okhttp
ARG FLINK_RUNTIME_SOURCE_DIR=sources/flink/flink-1-18-src-from-justin
ARG ROCKSDBJNI_JAR=rocksdbjni-6.20.3-linux64.jar

WORKDIR /app

COPY ${FLINK_RUNTIME_SOURCE_DIR}/ .

RUN --mount=type=cache,target=/root/.m2 \
  mvn install:install-file \
    -Dfile=${ROCKSDBJNI_JAR} \
    -DgroupId=org.rocksdb \
    -DartifactId=frocksdbjni \
    -Dversion=6.20.3-custom \
    -Dpackaging=jar
RUN --mount=type=cache,target=/root/.m2 mvn clean install -T 8 -DskipTests -Dspotless.check.skip=true -Drat.skip=true -Dcheckstyle.skip

FROM ghcr.io/apache/flink-docker:1.18-SNAPSHOT-scala_2.12-java11-debian

RUN rm -rf /opt/flink
USER flink
COPY --from=build --chown=flink:flink /app/flink-dist/target/flink-1.18-SNAPSHOT-bin/flink-1.18-SNAPSHOT/ /opt/flink
RUN mkdir /opt/flink/plugins/flink-s3-fs-hadoop; \
  cp /opt/flink/opt/flink-s3-fs-hadoop-1.18-SNAPSHOT.jar /opt/flink/plugins/flink-s3-fs-hadoop/
ENV FLINK_HOME=/opt/flink

RUN sed -i 's/rest.address: localhost/rest.address: 0.0.0.0/g' $FLINK_HOME/conf/flink-conf.yaml; \
  sed -i 's/rest.bind-address: localhost/rest.bind-address: 0.0.0.0/g' $FLINK_HOME/conf/flink-conf.yaml; \
  sed -i 's/jobmanager.bind-host: localhost/jobmanager.bind-host: 0.0.0.0/g' $FLINK_HOME/conf/flink-conf.yaml; \
  sed -i 's/taskmanager.bind-host: localhost/taskmanager.bind-host: 0.0.0.0/g' $FLINK_HOME/conf/flink-conf.yaml; \
  sed -i '/taskmanager.host: localhost/d' $FLINK_HOME/conf/flink-conf.yaml;
