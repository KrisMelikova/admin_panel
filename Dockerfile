FROM python:3.10-slim

WORKDIR /opt/app

ARG USERNAME=myuser
ARG USER_UID=1000
ARG USER_GID=$USER_UID

# добаляется группа и юзер с одним и тем же названием
# назначаем USER_UID 1000, чтобы совпадал с локальным юзером, это решает проблемы с монтированием
# -m - create the user's home directory if it does not exist
RUN groupadd --gid $USER_GID $USERNAME && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV UWSGI_PROCESSES 1
ENV UWSGI_THREADS 16
ENV UWSGI_HARAKIRI 240
ENV DJANGO_SETTINGS_MODULE 'movies_app.settings'

COPY movies_app/run_uwsgi.sh run_uwsgi.sh
COPY requirements.txt requirements.txt
COPY movies_app/uwsgi/uwsgi.ini uwsgi.ini

RUN apt update && apt install -y gcc && pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

COPY movies_app movies_app
COPY sqlite_to_postgres sqlite_to_postgres
COPY entrypoint.sh entrypoint.sh

RUN chown myuser:myuser -R sqlite_to_postgres/load_data.py
RUN chmod +x entrypoint.sh

EXPOSE 8000

USER myuser

ENTRYPOINT ["./entrypoint.sh"]
