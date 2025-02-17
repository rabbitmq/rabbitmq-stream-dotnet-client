#!/bin/bash

set -o errexit
set -o pipefail
set -o xtrace

script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
readonly script_dir
echo "[INFO] script_dir: '$script_dir'"

readonly docker_name_prefix='rabbitmq-stream-dotnet-client'
readonly docker_network_name="$docker_name_prefix-network"

if [[ ! -v GITHUB_ACTIONS ]]
then
    GITHUB_ACTIONS='false'
fi

if [[ -d $GITHUB_WORKSPACE ]]
then
    echo "[INFO] GITHUB_WORKSPACE is set: '$GITHUB_WORKSPACE'"
else
    GITHUB_WORKSPACE="$(readlink --canonicalize-existing "$script_dir/../..")"
    echo "[INFO] set GITHUB_WORKSPACE to: '$GITHUB_WORKSPACE'"
fi

declare -r rabbitmq_docker_name="$docker_name_prefix-rabbitmq"

if [[ $1 == 'stop' ]]
then
    docker stop "$rabbitmq_docker_name"
    exit $?
fi

if [[ $1 == 'pull' ]]
then
    readonly docker_pull_args='--pull always'
else
    readonly docker_pull_args=''
fi

set -o nounset

function start_rabbitmq
{
    echo "[INFO] starting RabbitMQ server docker container"
    chmod 0777 "$GITHUB_WORKSPACE/.ci/ubuntu/log"
    docker rm --force "$rabbitmq_docker_name" 2>/dev/null || echo "[INFO] $rabbitmq_docker_name was not running"
    # shellcheck disable=SC2086
    docker run --detach $docker_pull_args \
        --name "$rabbitmq_docker_name" \
        --hostname "$rabbitmq_docker_name" \
        --publish 5552:5552 \
        --publish 5672:5672 \
        --publish 15672:15672 \
        --publish 1883:1883 \
        --publish 61613:61613 \
        --network "$docker_network_name" \
        --env RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS='-rabbitmq_stream advertised_host localhost -rabbit collect_statistics_interval 4' \
        --volume "$GITHUB_WORKSPACE/.ci/ubuntu/enabled_plugins:/etc/rabbitmq/enabled_plugins" \
        --volume "$GITHUB_WORKSPACE/.ci/ubuntu/rabbitmq.conf:/etc/rabbitmq/rabbitmq.conf:ro" \
        --volume "$GITHUB_WORKSPACE/.ci/ubuntu/log:/var/log/rabbitmq" \
        rabbitmq:management
}

function wait_rabbitmq
{
    set +o errexit
    set +o xtrace

    declare -i count=12
    while (( count > 0 )) && [[ "$(docker inspect --format='{{.State.Running}}' "$rabbitmq_docker_name")" != 'true' ]]
    do
        echo '[WARNING] RabbitMQ container is not yet running...'
        sleep 5
        (( count-- ))
    done

    declare -i count=12
    while (( count > 0 )) && ! docker exec "$rabbitmq_docker_name" epmd -names | grep -F 'name rabbit'
    do
        echo '[WARNING] epmd is not reporting rabbit name just yet...'
        sleep 5
        (( count-- ))
    done

    docker exec "$rabbitmq_docker_name" rabbitmqctl await_startup

    set -o errexit
    set -o xtrace
}

function get_rabbitmq_id
{
    local rabbitmq_docker_id
    rabbitmq_docker_id="$(docker inspect --format='{{.Id}}' "$rabbitmq_docker_name")"
    echo "[INFO] '$rabbitmq_docker_name' docker id is '$rabbitmq_docker_id'"
    if [[ -v GITHUB_OUTPUT ]]
    then
        if [[ -f $GITHUB_OUTPUT ]]
        then
            echo "[INFO] GITHUB_OUTPUT file: '$GITHUB_OUTPUT'"
        fi
        echo "id=$rabbitmq_docker_id" >> "$GITHUB_OUTPUT"
    fi
}

docker network create "$docker_network_name" || echo "[INFO] network '$docker_network_name' is already created"

start_rabbitmq

wait_rabbitmq

get_rabbitmq_id # Note: unused for now
