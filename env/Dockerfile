# ==========================================================
# Base: Rocky Linux 8 (modern glibc/libstdc++)
# Java 17 (Amazon Corretto) + Scala 2.13.17 + sbt 1.11.7
# ==========================================================
FROM rockylinux:8

# Basic utilities
RUN dnf install -y wget curl tar git unzip which && dnf clean all

# Install Amazon Corretto JDK 17
RUN curl -L -o /tmp/corretto.rpm https://corretto.aws/downloads/latest/amazon-corretto-17-x64-linux-jdk.rpm && \
    dnf localinstall -y /tmp/corretto.rpm && \
    rm -f /tmp/corretto.rpm && \
    dnf clean all

ENV JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto
ENV PATH=$JAVA_HOME/bin:$PATH

# Install Scala 2.13.17
ARG SCALA_VERSION=2.13.17
RUN curl -fSL https://github.com/scala/scala/releases/download/v${SCALA_VERSION}/scala-${SCALA_VERSION}.tgz -o /tmp/scala.tgz && \
    tar -xzf /tmp/scala.tgz -C /usr/local && \
    ln -s /usr/local/scala-${SCALA_VERSION} /usr/local/scala && \
    rm -f /tmp/scala.tgz
ENV SCALA_HOME=/usr/local/scala
ENV PATH=$SCALA_HOME/bin:$PATH

# Install sbt 1.11.7
RUN curl -L -o /tmp/sbt.tgz https://github.com/sbt/sbt/releases/download/v1.11.7/sbt-1.11.7.tgz && \
    tar -xzf /tmp/sbt.tgz -C /usr/local && \
    ln -s /usr/local/sbt/bin/sbt /usr/local/bin/sbt && \
    rm -f /tmp/sbt.tgz

# Working directory
WORKDIR /app

CMD ["/bin/bash"]

