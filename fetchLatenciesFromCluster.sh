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

IP=10.34.58.70
FOLDER=streamteam-evaluation

#http://stackoverflow.com/questions/59895/getting-the-source-directory-of-a-bash-script-from-within
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

cd $DIR

ssh -i ~/.ssh/lukasPMAAS ubuntu@$IP "rm -r $FOLDER/latencies"
ssh -i ~/.ssh/lukasPMAAS ubuntu@$IP "$FOLDER/calculateLatencies.sh"
ssh -i ~/.ssh/lukasPMAAS ubuntu@$IP "cd $FOLDER; tar -cf latencies.tar latencies"
scp -i ~/.ssh/lukasPMAAS ubuntu@$IP:$FOLDER/latencies.tar ./
tar -xf latencies.tar
ssh -i ~/.ssh/lukasPMAAS ubuntu@$IP "rm $FOLDER/latencies.tar"
rm latencies.tar
