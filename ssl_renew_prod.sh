#!/bin/bash

COMPOSE="sudo -u seth docker compose -f docker-compose.yml -f docker-compose.prod.yml --ansi never"

cd /home/seth/regrail/
$COMPOSE run certbot renew
$COMPOSE kill -s SIGHUP webserver

docker system prune -af
