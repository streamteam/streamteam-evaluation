#!/bin/bash

#
# StreamTeam
# Copyright (C) 2019  University of Basel
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

die() {
	echo >&2 "$@"
	exit 1
}
[ "$#" -ge 3 ] || die "requires at least three arguments (sensorSimulatorNodesLine, match, maxNumberOfSimulations), $# provided"

line=$1
match=$2
maxNumberOfSimulations=$3

#http://stackoverflow.com/questions/59895/getting-the-source-directory-of-a-bash-script-from-within
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

cd $DIR

rm -R ./evaluationRowOutput
mkdir ./evaluationRowOutput

#https://www.cyberciti.biz/faq/bash-for-loop/
for (( i=1; i<=$maxNumberOfSimulations; i++))
do
	echo "Reboot all cluster machines"
	~/streamteam-cluster-scripts/rebootAll.sh
	echo "Sleep for 10 minutes"
	sleep 10m
	
	echo "Start cluster stack"
	~/streamteam-cluster-scripts/startClusterStack.sh
	echo "Sleep for 2 minutes"
	sleep 2m

	echo "Deploy and run StreamTeam-Football"
	../streamteam-data-stream-analysis-system/deployAndRunFootballDemo.sh
	echo "Sleep for 2 minutes"
	sleep 2m

	echo "Evaluate for "$i" concurrent matches."	
	./evaluate.sh $line $match $i

	echo "Copy latencies and stats to ./evaluationRowOutput/"$i
	mkdir ./evaluationRowOutput/$i
	mkdir ./evaluationRowOutput/$i/latencies
	mkdir ./evaluationRowOutput/$i/stats
	cp ./latencies/* ./evaluationRowOutput/$i/latencies
	cp ./stats/* ./evaluationRowOutput/$i/stats
done

echo "Finished evaluation row"

./createEvaluationRowStatistics.py $maxNumberOfSimulations
echo "Created row statistics"
