FROM python:3

WORKDIR /app

COPY ./app/requirements.txt .

RUN pip install --upgrade pip  \
  && pip install -r requirements.txt \
  && apt-get update \
  && apt-get apt-get install vim
RUN git clone https://github.com/Parkman328/QRAG-Docker.git
COPY /app/install/sagemaker/qrag.ini /app/QRAG-Docker/sagemaker/config
COPY /app/install/precog/qrag.ini /app/QRAG-Docker/precog/config
#CMD python ../sagemaker
