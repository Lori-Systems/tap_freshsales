FROM lorisystems/python32

RUN apt-get update -y \
    && apt-get install -y build-essential \
    && apt-get install libpq-dev -y 
RUN apt install unixodbc-dev -y && apt install python-pyodbc -y 
RUN apt install git -y
WORKDIR /app
COPY requirements.txt requirements.txt
ARG PIP_EXTRA_INDEX_URL
RUN pip install -r requirements.txt
RUN apt install python3-pip -y
RUN unset PIP_EXTRA_INDEX_URL &&  pip3 install target-redshift
COPY . .
CMD python2 /usr/local/lib/python2.7/site-packages/tap_freshsales/__init__.py --config config.json | target-redshift -c db_config_redshift.json
