#!/bin/bash - 

exec java -cp $(dirname $0)/uber-koma-${project.version}.jar com.infonova.opss.koma.Koma "$@"
