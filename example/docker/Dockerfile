FROM public.ecr.aws/emr-serverless/spark/emr-6.12.0:latest

USER root
RUN yum install -y bzip2

USER hadoop:hadoop
WORKDIR /home/hadoop

RUN pip3 install metaflow-netflixext==1.0.3
RUN curl -Ls https://micro.mamba.pm/api/micromamba/linux-64/latest | tar -xvj bin/micromamba

COPY trampoline.py metaflow-python.sh /home/hadoop

ENV PATH=~/bin:$PATH
