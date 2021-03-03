FROM elixir:alpine

# need build-utils for building deps, bash for QOL
RUN apk update && \
  apk add build-base bash git openssh inotify-tools

RUN mkdir -p /app/kaufmann
WORKDIR /app/kaufmann

ADD . .

RUN mix do local.hex --force, \
  local.rebar --force, \
  deps.get
CMD /bin/bash
