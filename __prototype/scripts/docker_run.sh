#!/bin/bash
cd "$(dirname "$0")"/../docker || exit 1

docker compose up -d