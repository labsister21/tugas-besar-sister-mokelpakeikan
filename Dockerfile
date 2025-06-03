FROM openjdk:11-jre-slim

WORKDIR /app
COPY raft/target/raft-1.0-SNAPSHOT-jar-with-dependencies.jar ./target/

EXPOSE 8001 8002 8003 8004 