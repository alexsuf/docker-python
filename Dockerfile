FROM alexsuf/fedora

RUN yum -y install pip
RUN dnf install python3
RUN pip install psycopg2-binary && pip install greenplum-python && pip install pg8000
RUN pip install flask
RUN yum -y update


CMD ["/bin/bash"]