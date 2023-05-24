FROM python:3.9

RUN pip install mysql-connector-python
RUN pip install jupyterlab
RUN pip install psycopg2-binary
RUN pip install pymongo

WORKDIR /user/app

COPY hello_world.py ./