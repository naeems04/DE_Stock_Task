FROM python:3

COPY . /app

WORKDIR /app

# Service must listen to $PORT environment variable.
# This default value facilitates local development.
EXPOSE 8080

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

CMD [ "python", "connector.py"]