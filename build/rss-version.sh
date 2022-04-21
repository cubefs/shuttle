#!/usr/bin/env bash
BASE_DIR="$1"
mkdir -p $BASE_DIR
VERSION_INFO="${BASE_DIR}"/rss-build-version.properties

echo "Creating build version info: " $VERSION_INFO

create_verion() {
  echo project_version=$1
  echo git_commit_version=$(git rev-parse HEAD)
}

create_verion $2 > $VERSION_INFO


