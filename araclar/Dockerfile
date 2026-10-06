FROM python:3.12-slim
# Üretici, doğrulama ve arama örneği için gereken Python kütüphaneleri.
# Bilgisayarınıza Python kurmanız gerekmez; hepsi bu kapsayıcının içinde çalışır.
RUN pip install --no-cache-dir \
      confluent-kafka==2.6.1 \
      boto3==1.35.54 \
      pandas==2.2.3 \
      pyarrow==18.0.0 \
      duckdb==1.1.3 \
      elasticsearch==8.15.1 \
      pymongo==4.10.1
WORKDIR /kit
