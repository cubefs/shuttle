#!/usr/bin/env bash

RSS_HOME="$(cd "`dirname "$0"`/.."; pwd)"
DIST_DIR="$RSS_HOME/build/dist"
DIST_ZIP=shuttle-rss.zip
CONF_DIR=$1


# Compile the jar package
cd "$RSS_HOME"
mvn clean package -Pserver -DskipTests


# Copy files
rm -rf "$DIST_DIR"
mkdir -p "$DIST_DIR"/lib
mkdir -p "$DIST_DIR"/conf
mkdir -p "$DIST_DIR"/bin
mkdir -p "$DIST_DIR"/client

cp "$RSS_HOME"/target/shuttle-rss-*-server.jar "$DIST_DIR"/lib
cp "$RSS_HOME"/target/shuttle-rss-*-client.jar "$DIST_DIR"/client

# If the configuration file directory is known, the configuration file will be copied.
# Otherwise the configuration will be downloaded from the cloud platform configuration center
# The configuration file usually contains the following:
# 1. rss_env.sh Environment variable
# 2. core-site.xml、hdfs-site.xml、cfs-site.xml、alluxio-site.xml
if [ -n "$CONF_DIR" ]; then
    cp "$CONF_DIR"/* "$DIST_DIR"/conf
  else
    cp "$RSS_HOME"/conf/* "$DIST_DIR"/conf
fi

cp "${RSS_HOME}"/bin/* "$DIST_DIR"/bin


# Create zip package
cd "$DIST_DIR"
zip -m -r "$DIST_ZIP" *


echo "build success, file: $DIST_DIR/$DIST_ZIP"
