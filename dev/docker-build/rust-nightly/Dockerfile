FROM centos:7

# install gcc-11
RUN yum update -y
RUN yum install -y centos-release-scl
RUN yum install -y devtoolset-11-gcc
RUN echo '. /opt/rh/devtoolset-11/enable' >> ~/.bashrc

# install rust stable toolchain
RUN curl https://sh.rustup.rs > /rustup-init
RUN chmod +x /rustup-init
RUN /rustup-init -y --default-toolchain nightly-x86_64-unknown-linux-gnu
