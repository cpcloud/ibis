#!/bin/bash
base="https://circleci.com/api/v1.1"
url="project/github/ibis-project/ibis"
project_url="${base}/${url}"

build_nums=($(curl -L -s "${project_url}" \
    | jq '.[] | {name: .workflows.job_name, build_num: .build_num} | select(.name | contains("conda_build"))' \
    | jq -s 'sort_by(.name, -.build_num) | group_by(.name) | .[][0].build_num'))

channels=()

for build_num in "${build_nums[@]}"; do
    channel="$(
    curl -L -s "${project_url}/${build_num}/artifacts" \
	| jq -r '.[].url' \
	| sed -e 'N;s/^\(.*\).*\n\1.*$/\1\n\1/;D')"
    channels+=("--channel ${channel}")
done

conda install ibis-framework ${channels[*]}
