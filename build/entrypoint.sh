#!/bin/bash

ROLE_TYPE=$1

case "$ROLE_TYPE" in
    (master)
       /bin/bash ${SHUTTLE_RSS_HOME}/bin/run_master.sh start
       tail -f /dev/null &
      ;;

    (worker)
      /bin/bash ${SHUTTLE_RSS_HOME}/bin/run_worker.sh start
      tail -f /dev/null &
      ;;
    (all)
      /bin/bash ${SHUTTLE_RSS_HOME}/bin/run_master.sh start
      /bin/bash ${SHUTTLE_RSS_HOME}/bin/run_worker.sh start
      tail -f /dev/null &
      ;;
    (*)
      exec "$@"
      ;;
esac

wait || :