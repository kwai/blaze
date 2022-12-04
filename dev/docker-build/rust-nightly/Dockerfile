FROM centos:7

# fast rebuild the image
RUN echo '483978A0-C70C-4EAD-8854-ED405B0C77EF'

RUN yum update -y
RUN yum install -y centos-release-scl

# install common tools
RUN yum install -y libzip unzip wget

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

# install protoc
RUN wget -O /protobuf-21.7-linux-x86_64.zip https://github.com/protocolbuffers/protobuf/releases/download/v21.7/protoc-21.7-linux-x86_64.zip
RUN mkdir /protobuf-bin && (cd /protobuf-bin && unzip /protobuf-21.7-linux-x86_64.zip)
RUN echo 'export PATH="$PATH:/protobuf-bin/bin"' >> ~/.bashrc
