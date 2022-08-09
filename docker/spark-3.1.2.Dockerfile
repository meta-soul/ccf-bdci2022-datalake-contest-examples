FROM ubuntu:20.04

SHELL ["/bin/bash", "-c"]

ENV DEBIAN_FRONTEND=noninteractive
RUN sed -i "s@http://.*archive.ubuntu.com@http://repo.huaweicloud.com@g" /etc/apt/sources.list \
    && sed -i "s@http://.*security.ubuntu.com@http://repo.huaweicloud.com@g" /etc/apt/sources.list
RUN apt-get update && apt-get upgrade -y && apt-get clean
RUN apt-get install -y locales && apt-get clean
RUN sed -i '/en_US.UTF-8/s/^# //g' /etc/locale.gen && \
    locale-gen
ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US.UTF-8
ENV LC_CTYPE=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8
ENV TZ=Asia/Shanghai
RUN ln -svf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && apt-get install -y tzdata && apt-get clean
RUN apt-get install -y openjdk-11-jdk maven \
    tini python3.8 python3-setuptools ca-certificates openjdk-11-jre \
    wget curl git \
    && apt-get clean
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3 30
RUN python -m easy_install install pip
RUN python -m pip config set global.index-url https://repo.huaweicloud.com/repository/pypi/simple
RUN python -m pip install pyarrow pandas py4j==0.10.9 --no-cache-dir

RUN mkdir -p /opt/spark \
    && wget -q https://dmetasoul-bucket.ks3-cn-beijing.ksyuncs.com/releases/spark/spark-3.1.2-bin-free-265f9ad4ee.tgz \
    && tar xf spark-3.1.2-bin-free-265f9ad4ee.tgz -C /opt/spark --strip-components=1 \
    && rm -f spark-3.1.2-bin-free-265f9ad4ee.tgz

ENV SPARK_HOME /opt/spark
RUN mkdir -p /opt/spark/conf
ENV SPARK_CONF_DIR /opt/spark/conf
ENV PATH=$SPARK_HOME/bin:$PATH
ENV JAVA_HOME=/usr
ENV PYTHONPATH=/opt/spark/python:$PYTHONPATH
RUN rm -f $SPARK_HOME/jars/HikariCP*
RUN mkdir -p /opt/spark/work-dir
RUN wget https://raw.githubusercontent.com/apache/spark/v3.1.2/resource-managers/kubernetes/docker/src/main/dockerfiles/spark/entrypoint.sh -O /opt/entrypoint.sh
RUN wget https://raw.githubusercontent.com/apache/spark/v3.1.2/resource-managers/kubernetes/docker/src/main/dockerfiles/spark/decom.sh -O /opt/decom.sh
WORKDIR /opt/spark/work-dir
RUN chmod g+w /opt/spark/work-dir
RUN chmod a+x /opt/decom.sh
RUN chmod a+x /opt/entrypoint.sh
RUN chgrp root /etc/passwd && chmod ug+rw /etc/passwd
ENTRYPOINT [ "/opt/entrypoint.sh" ]
