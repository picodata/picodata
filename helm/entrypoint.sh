#!/bin/sh
sleep 3

if [ -z "${INSTANCE_NAME}" ]; then
    INSTANCE_NAME="$(hostname)"
fi

if [ -z "${POD_IP}" ]; then
    POD_IP="0.0.0.0"
fi

if [ -z "${POD_PORT}" ]; then
    POD_PORT=3301
fi

if [ -z "${PEER_URI}" ]; then
    PEER_URI="${INSTANCE_NAME}:${POD_PORT}"
fi

if [ -z "${ADVERTISE_URI}" ]; then
    ADVERTISE_URI="${INSTANCE_NAME}:${POD_PORT}"
fi

export PICODATA_INSTANCE_ID="${INSTANCE_NAME}"
export PICODATA_LISTEN="${POD_IP}:${POD_PORT}"
export PICODATA_PEER="${PEER_URI}"
export PICODATA_ADVERTISE="${ADVERTISE_URI}"

exec /usr/bin/picodata run "$@"
