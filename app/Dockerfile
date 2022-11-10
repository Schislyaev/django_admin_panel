FROM python:3.10.7

WORKDIR /opt/app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV UWSGI_PROCESSES 1
ENV UWSGI_THREADS 16
ENV UWSGI_HARAKIRI 240
ENV DJANGO_SETTINGS_MODULE 'config.settings'

COPY run_uwsgi.sh run_uwsgi.sh
COPY requirements.txt requirements.txt
COPY uwsgi/uwsgi.ini uwsgi.ini

USER root
RUN apt update && apt install -y netcat
RUN  mkdir -p /opt/app/static/ \
     && pip install --upgrade pip \
     && pip install -r requirements.txt

COPY ./entrypoint.sh /root/entrypoint.sh

COPY . .

EXPOSE 8000

RUN chmod +x /root/entrypoint.sh
ENTRYPOINT ["/root/entrypoint.sh"]
