#!/bin/sh

SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")
PARENT=$(dirname "$SCRIPTPATH")
DATOMIC=$PARENT/datomic-pro-1.0.6165

K=${1:-myaccesskey}
S=${2:-mysecret}
DB=${3:-hello}

$DATOMIC/bin/transactor $PARENT/datomics-transactor.properties &

$DATOMIC/bin/run -m datomic.peer-server -h localhost -p 8998 -a $K,$S -d $DB,datomic:dev://localhost:4334/$DB &

$DATOMIC/bin/console -p 8080 dev datomic:dev://localhost:4334/ &

