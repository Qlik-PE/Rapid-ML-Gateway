FROM python:3

WORKDIR /app

COPY ./app/requirements.txt .

RUN pip install --upgrade pip  \
  && pip install -r requirements.txt 
RUN git clone https://github.com/Parkman328/QRAG-Docker.git
COPY /app/install/sagemaker/qrag.ini /app/QRAG-Docker/sagemaker/config
CMD python QRAG-Docker/sagemaker/
#CMD python ../sagemaker
