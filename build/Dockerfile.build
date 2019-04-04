FROM centos:7
LABEL version=0.0.10

RUN yum install -y java-1.8.0-openjdk-devel python git unzip wget which time
RUN yum install -y https://www.foundationdb.org/downloads/6.0.15/rhel6/installers/foundationdb-clients-6.0.15-1.el6.x86_64.rpm nmap

RUN mkdir -p /usr/local/bin
COPY fdb_create_cluster_file.bash /usr/local/bin/fdb_create_cluster_file.bash

ENV PATH="${PATH}:/opt/gradle/gradle-3.4.1/bin:/usr/local/bin"
ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk
