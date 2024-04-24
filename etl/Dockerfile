FROM python:3.10-slim

WORKDIR /opt/app

ARG USERNAME=myuser
ARG USER_UID=1000
ARG USER_GID=$USER_UID

RUN groupadd --gid $USER_GID $USERNAME && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME

COPY requirements.txt requirements.txt

RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

COPY . .

RUN chown myuser:myuser -R storage/storage.json

USER myuser

ENTRYPOINT ["sh", "-c", "sleep 10 && python3 main.py"]