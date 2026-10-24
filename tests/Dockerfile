ARG PHP_VERSION=8.2
ARG BASE_IMAGE=php:${PHP_VERSION}-cli
FROM ${BASE_IMAGE}
RUN apt-get update && apt-get install -y --no-install-recommends unzip \
    && docker-php-ext-install pcntl \
    && pecl install redis && docker-php-ext-enable redis \
    && rm -rf /var/lib/apt/lists/*
RUN curl -fsSL https://getcomposer.org/download/latest-stable/composer.phar -o /usr/local/bin/composer \
    && chmod +x /usr/local/bin/composer
WORKDIR /package
