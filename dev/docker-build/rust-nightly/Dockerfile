FROM centos:7

RUN yum update -y
RUN yum install -y centos-release-scl

# install common tools
RUN yum install -y unzip wget

# install gcc-11
RUN yum install -y devtoolset-11-gcc devtoolset-11-gcc-c++
RUN echo '. /opt/rh/devtoolset-11/enable' >> ~/.bashrc

# install rust nightly toolchain
RUN curl https://sh.rustup.rs > /rustup-init
RUN chmod +x /rustup-init
RUN /rustup-init -y --default-toolchain nightly-x86_64-unknown-linux-gnu

# install java/gradle
RUN yum install -y java-11-openjdk-devel
RUN echo 'export JAVA_HOME="/usr/lib/jvm/java-11-openjdk"' >> ~/.bashrc

# install gradle
RUN wget -O /gradle-7.4.2-bin.zip https://mirrors.huaweicloud.com/gradle/gradle-7.4.2-bin.zip
RUN mkdir /gradle-bin && (cd /gradle-bin && unzip /gradle-7.4.2-bin.zip)
RUN echo 'export PATH="$PATH:/gradle-bin/bin"' >> ~/.bashrc
