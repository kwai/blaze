#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

FROM centos:7

# install common tools
RUN echo "sslverify=false" >> /etc/yum.conf
RUN sed -i "s/mirror.centos.org/vault.centos.org/g" /etc/yum.repos.d/*.repo
RUN sed -i "s/^#.*baseurl=http/baseurl=https/g" /etc/yum.repos.d/*.repo
RUN sed -i "s/^mirrorlist/#mirrorlist/g" /etc/yum.repos.d/*.repo
RUN yum update -y
RUN yum install -y centos-release-scl epel-release
RUN sed -i "s/mirror.centos.org/vault.centos.org/g" /etc/yum.repos.d/*.repo
RUN sed -i "s/^#.*baseurl=http/baseurl=https/g" /etc/yum.repos.d/*.repo
RUN sed -i "s/^mirrorlist/#mirrorlist/g" /etc/yum.repos.d/*.repo
RUN yum install -y libzip unzip wget cmake3 openssl-devel

# install gcc-11
RUN yum install -y devtoolset-11-gcc devtoolset-11-gcc-c++
RUN echo '. /opt/rh/devtoolset-11/enable' >> ~/.bashrc

# install rust nightly toolchain
RUN curl https://sh.rustup.rs > /rustup-init
RUN chmod +x /rustup-init
RUN /rustup-init -y --default-toolchain nightly-2023-08-01-x86_64-unknown-linux-gnu

# install java
RUN yum install -y java-1.8.0-openjdk java-1.8.0-openjdk-devel
RUN echo 'export JAVA_HOME="/usr/lib/jvm/java-1.8.0-openjdk"' >> ~/.bashrc
