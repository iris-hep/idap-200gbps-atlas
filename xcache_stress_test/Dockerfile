FROM opensciencegrid/xcache:3.6-release

RUN yum install -y xrootd-client
COPY 4xcaches.txt .
COPY 8xcaches.txt .
COPY dcache.txt .
COPY runme.sh .
COPY x509_proxy .
RUN chmod +x runme.sh
