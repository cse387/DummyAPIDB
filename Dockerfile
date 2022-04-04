
FROM python:3
#.7-alpine
#8-slim-buster

# ADD test.py /
# RUN pip install pandas
# CMD [ "python", "./test.py" ]
# COPY test.py /
ENV PYTHONUNBUFFERED=1

#RUN sed -i 's/\r//' /code
#RUN chmod +x /code

WORKDIR /code
# ENV STATIC_INDEX 1
ADD test.py /
COPY requirements.txt /code/
RUN pip install -r /code/requirements.txt
CMD [ "python", "./test.py" ]
# RUN python3 test.py
# RUN pip install dbt-redshift/.
COPY . /code/
# CMD python3 code.py

# WORKDIR /src/notebooks
# # Add Tini. Tini operates as a process subreaper for jupyter. This prevents kernel crashes.
# ENV TINI_VERSION v0.6.0
# ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /usr/bin/tini
# RUN chmod +x /usr/bin/tini
# ENTRYPOINT ["/usr/bin/tini", "--"]


#CMD ["jupyter", "notebook", "--port=8888", "--no-browser", "--ip=0.0.0.0", "--allow-root"]

#RUN pip install "apache-airflow[celery]==2.2.4" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.2.4/constraints-3.6.txt"

# FROM continuumio/miniconda3:4.5.11

# RUN apt-get update -y; apt-get upgrade -y; apt-get install -y vim-tiny vim-athena ssh

# COPY environment.yml environment.yml

# RUN conda env create -f environment.yml
# RUN echo "alias l='ls -lah'" >> ~/.bashrc
# RUN echo "source activate connect" >> ~/.bashrc

# # Setting these environmental variables is the functional equivalent of running 'source activate my-conda-env'
# ENV CONDA_EXE /opt/conda/bin/conda
# ENV CONDA_PREFIX /opt/conda/envs/connect
# ENV CONDA_PYTHON_EXE /opt/conda/bin/python
# ENV CONDA_PROMPT_MODIFIER (connect)
# ENV CONDA_DEFAULT_ENV connect
# ENV PATH /opt/conda/envs/connect/bin:/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin