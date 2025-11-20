FROM python:3.11

LABEL maintainer="amendoza39@bcit.ca"

RUN mkdir /app
COPY ./requirements.txt /app/requirements.txt
WORKDIR /app
RUN pip3 install --upgrade pip
RUN pip3 install setuptools
RUN pip3 install -r requirements.txt
COPY . /app
RUN chown -R nobody:nogroup /app
USER nobody

EXPOSE 8080

ENTRYPOINT [ "python3" ]
CMD [ "app.py" ]