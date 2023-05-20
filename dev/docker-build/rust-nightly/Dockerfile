FROM centos:7

# fast rebuild the image
RUN echo '08556F26-AB9F-4339-BB60-620D4042EEB5'

RUN yum update -y
RUN yum install -y centos-release-scl epel-release

# install common tools
RUN yum install -y libzip unzip wget cmake3 openssl-devel

# install gcc-11
RUN yum install -y devtoolset-11-gcc devtoolset-11-gcc-c++
RUN echo '. /opt/rh/devtoolset-11/enable' >> ~/.bashrc

# install rust nightly toolchain
RUN curl https://sh.rustup.rs > /rustup-init
RUN chmod +x /rustup-init
RUN /rustup-init -y --default-toolchain nightly-x86_64-unknown-linux-gnu

# install java/gradle
RUN yum install -y java-1.8.0-openjdk
RUN echo 'export JAVA_HOME="$(echo /usr/lib/jvm/java-1.8.0-openjdk*)"' >> ~/.bashrc

# install gradle
RUN wget -O /gradle-7.4.2-bin.zip https://mirrors.huaweicloud.com/gradle/gradle-7.4.2-bin.zip
RUN mkdir /gradle-bin && (cd /gradle-bin && unzip /gradle-7.4.2-bin.zip)
RUN echo 'export PATH="$PATH:/gradle-bin/bin"' >> ~/.bashrc

# install protoc
RUN wget -O /protobuf-21.7-linux-x86_64.zip https://github.com/protocolbuffers/protobuf/releases/download/v21.7/protoc-21.7-linux-x86_64.zip
RUN mkdir /protobuf-bin && (cd /protobuf-bin && unzip /protobuf-21.7-linux-x86_64.zip)
RUN echo 'export PATH="$PATH:/protobuf-bin/bin"' >> ~/.bashrc

# install mold linker
RUN wget -O /mold-v1.9.0.tar.gz https://github.com/rui314/mold/archive/refs/tags/v1.9.0.tar.gz
RUN tar -xf /mold-v1.9.0.tar.gz
RUN cd /mold-1.9.0 && source ~/.bashrc && cmake3 -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER=c++ . && make -j8 && make install

# use mold as rust's default linker
RUN ln -s /usr/local/bin/ld.mold /usr/bin/ld.lld
RUN echo '[target.x86_64-unknown-linux-gnu]' >> ~/.cargo/config
RUN echo 'rustflags = ["-C", "link-arg=-fuse-ld=lld"]' >> ~/.cargo/config

