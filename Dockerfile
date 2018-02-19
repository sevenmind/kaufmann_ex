From elixir:alpine
MAINTAINER Grant Mclendon <grant@7mind.de>

# need build-utils for building deps, bash for QOL
RUN apk update && \
    apk add build-base bash

RUN mkdir -p /app/7mind-micro-auth
WORKDIR /app/7mind-micro-auth

ADD . .

RUN mix do local.hex --force, \
           local.rebar --force, \
           deps.get
CMD /bin/bash
