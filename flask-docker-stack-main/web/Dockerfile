############################################################
# Dockerfile to build Flask App
# Based on
############################################################

# Set the base image
FROM debian:bullseye-slim
RUN usermod -u 1000 www-data
RUN groupmod -g 1000 www-data

# File Author / Maintainer
LABEL image.author="carlos.tighe@universityofgalway.ie"

RUN apt-get update && apt-get install -y apache2 \
    libapache2-mod-wsgi-py3 \
    python3 \
    python3-pip \
    && apt-get clean \ 
    && apt-get autoremove \
    && rm -rf /var/lib/apt/lists/*

# Copy over and install the requirements
COPY ./app/requirements.txt /var/www/apache-flask/app/requirements.txt

RUN pip install -r /var/www/apache-flask/app/requirements.txt
#COPY ./app /var/www/apache-flask/app

# Copy over the apache configuration file and enable the site
COPY ./apache-flask.conf /etc/apache2/sites-available/apache-flask.conf
# Copy over the wsgi file, run.py and the app
COPY ./apache-flask.wsgi /var/www/apache-flask/apache-flask.wsgi

RUN a2dissite 000-default.conf
RUN a2ensite apache-flask.conf
RUN a2enmod headers


# LINK apache config to docker logs.
RUN ln -sf /proc/self/fd/1 /var/log/apache2/access.log && \
    ln -sf /proc/self/fd/1 /var/log/apache2/error.log
#RUN chown -R www-data:www-data /var/www/apache-flask/app

#USER www-data



EXPOSE 80

WORKDIR /var/www/apache-flask/app


