# syntax=docker/dockerfile:1.6
# ^ needed for ADD --checksum=…

FROM node:20-alpine as builder
WORKDIR /app

# install dependencies
# There are some (transitive) dependencies that need a C++ compiler, so we build them in a separate stage.
RUN apk add --update \
	g++ \
	make \
	python3
ADD package.json /app
RUN npm install --production

# ---

FROM node:20-alpine
WORKDIR /app

LABEL org.opencontainers.image.title="vdv-453-nats-adapter"
LABEL org.opencontainers.image.description="An HTTP API for Berlin & Brandenburg public transport."
LABEL org.opencontainers.image.authors="Jannis R <mail@jannisr.de>"
LABEL org.opencontainers.image.documentation="https://github.com/derhuerst/vbb-rest/tree/7"

# install dependencies
COPY --from=builder /app/node_modules ./node_modules

# add source code
ADD . /app

# CLI some test
RUN ./cli.js --help >/dev/null

ENTRYPOINT [ "./cli.js"]
