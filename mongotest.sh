#!/usr/bin/env bash

sleep 5
${PWD}/mongodb-linux-x86_64-3.0.5/bin/mongo localhost:27017 --eval 'printjson(db.runCommand({"isMaster": 1}))'
