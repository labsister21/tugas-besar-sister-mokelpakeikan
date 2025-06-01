#!/bin/bash

if [ "$NODE_TYPE" = "server" ]; then
    java -cp raft-kvstore-1.0-SNAPSHOT.jar com.raft.server.RaftServerMain $SERVER_ID
elif [ "$NODE_TYPE" = "client" ]; then
    java -cp raft-kvstore-1.0-SNAPSHOT.jar com.raft.cli.RaftCLI
else
    echo "Invalid NODE_TYPE. Must be 'server' or 'client'"
    exit 1
fi 