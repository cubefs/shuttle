FROM openjdk:8-jre-slim

ENV SHUTTLE_RSS_HOME=/usr/local/shuttle-rss
WORKDIR $SHUTTLE_RSS_HOME

RUN apt-get update && apt-get -y install zip  && apt-get -y install procps

ADD dist/shuttle-rss.zip .
RUN unzip shuttle-rss.zip
RUN rm -rf shuttle-rss.zip
RUN chmod +x bin/*

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh


EXPOSE 19189
EXPOSE 19188
EXPOSE 19190
EXPOSE 19191

ENTRYPOINT ["/entrypoint.sh"]