#!/usr/bin/python

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

import json
import os
import urllib

ip = '10.34.58.65'
timeWindow = '45m'

# https://docs.python.org/2/library/urllib2.html
processNsJsonString = urllib.urlopen('http://' + ip + ':9090/api/v1/query?query=avg(avg_over_time(samza:org_apache_samza_container_SamzaContainerMetrics:process_ns[' + timeWindow + ']))%20by%20(samza_job)').read()
windowNsJsonString = urllib.urlopen('http://' + ip + ':9090/api/v1/query?query=avg(avg_over_time(samza:org_apache_samza_container_SamzaContainerMetrics:window_ns[' + timeWindow + ']))%20by%20(samza_job)').read()
avgProcessCallsJsonString = urllib.urlopen('http://' + ip + ':9090/api/v1/query?query=avg(((sum_over_time(samza:org_apache_samza_container_SamzaContainerMetrics:process_calls[' + timeWindow + '])-(min_over_time(samza:org_apache_samza_container_SamzaContainerMetrics:process_calls[' + timeWindow + '])*count_over_time(samza:org_apache_samza_container_SamzaContainerMetrics:process_calls[' + timeWindow + '])))/count_over_time(samza:org_apache_samza_container_SamzaContainerMetrics:process_calls[' + timeWindow + ']))/(count_over_time(samza:org_apache_samza_container_SamzaContainerMetrics:process_calls[' + timeWindow + '])/2))%20by%20(samza_job)').read()
avgWindowCallsJsonString = urllib.urlopen('http://' + ip + ':9090/api/v1/query?query=avg(((sum_over_time(samza:org_apache_samza_container_SamzaContainerMetrics:window_calls[' + timeWindow + '])-(min_over_time(samza:org_apache_samza_container_SamzaContainerMetrics:window_calls[' + timeWindow + '])*count_over_time(samza:org_apache_samza_container_SamzaContainerMetrics:window_calls[' + timeWindow + '])))/count_over_time(samza:org_apache_samza_container_SamzaContainerMetrics:window_calls[' + timeWindow + ']))/(count_over_time(samza:org_apache_samza_container_SamzaContainerMetrics:window_calls[' + timeWindow + '])/2))%20by%20(samza_job)').read()
sumProcessCallsJsonString = urllib.urlopen('http://' + ip + ':9090/api/v1/query?query=sum(((sum_over_time(samza:org_apache_samza_container_SamzaContainerMetrics:process_calls[' + timeWindow + '])-(min_over_time(samza:org_apache_samza_container_SamzaContainerMetrics:process_calls[' + timeWindow + '])*count_over_time(samza:org_apache_samza_container_SamzaContainerMetrics:process_calls[' + timeWindow + '])))/count_over_time(samza:org_apache_samza_container_SamzaContainerMetrics:process_calls[' + timeWindow + ']))/(count_over_time(samza:org_apache_samza_container_SamzaContainerMetrics:process_calls[' + timeWindow + '])/2))%20by%20(samza_job)%20').read()
sumWindowCallsJsonString = urllib.urlopen('http://' + ip + ':9090/api/v1/query?query=sum(((sum_over_time(samza:org_apache_samza_container_SamzaContainerMetrics:window_calls[' + timeWindow + '])-(min_over_time(samza:org_apache_samza_container_SamzaContainerMetrics:window_calls[' + timeWindow + '])*count_over_time(samza:org_apache_samza_container_SamzaContainerMetrics:window_calls[' + timeWindow + '])))/count_over_time(samza:org_apache_samza_container_SamzaContainerMetrics:window_calls[' + timeWindow + ']))/(count_over_time(samza:org_apache_samza_container_SamzaContainerMetrics:window_calls[' + timeWindow + '])/2))%20by%20(samza_job)%20').read()

# Samza metric notes:
# * Samza's process-ns and window-ns metric are averages of the last 300 seconds (see http://samza.apache.org/learn/documentation/0.13/container/metrics-table.html)
# * Unclear why process-calls and window-calls require an additional division by count/2 but this works for arbitrary time windows

# https://www.w3schools.com/python/python_json.asp
processNsJsonObj = json.loads(processNsJsonString)
windowNsJsonObj = json.loads(windowNsJsonString)
avgProcessCallsJsonObj = json.loads(avgProcessCallsJsonString)
avgWindowCallsJsonObj = json.loads(avgWindowCallsJsonString)
sumProcessCallsJsonObj = json.loads(sumProcessCallsJsonString)
sumWindowCallsJsonObj = json.loads(sumWindowCallsJsonString)

processNsJsonObjResult = processNsJsonObj['data']['result']
windowNsJsonObjResult = windowNsJsonObj['data']['result']
avgProcessCallsJsonObjResult = avgProcessCallsJsonObj['data']['result']
avgWindowCallsJsonObjResult = avgWindowCallsJsonObj['data']['result']
sumProcessCallsJsonObjResult = sumProcessCallsJsonObj['data']['result']
sumWindowCallsJsonObjResult = sumWindowCallsJsonObj['data']['result']

# https://stackoverflow.com/questions/8703496/hash-map-in-python
processNsMap = {}
windowNsMap = {}
avgProcessCallsMap = {}
avgWindowCallsMap = {}
sumProcessCallsMap = {}
sumWindowCallsMap = {}

for result in processNsJsonObjResult:
    processNsMap[result['metric']['samza_job']] = result['value'][1]

for result in windowNsJsonObjResult:
    windowNsMap[result['metric']['samza_job']] = result['value'][1]

for result in avgProcessCallsJsonObjResult:
    avgProcessCallsMap[result['metric']['samza_job']] = result['value'][1]

for result in avgWindowCallsJsonObjResult:
    avgWindowCallsMap[result['metric']['samza_job']] = result['value'][1]

for result in sumProcessCallsJsonObjResult:
    sumProcessCallsMap[result['metric']['samza_job']] = result['value'][1]

for result in sumWindowCallsJsonObjResult:
    sumWindowCallsMap[result['metric']['samza_job']] = result['value'][1]

# https://www.pythonforbeginners.com/files/reading-and-writing-files-in-python & https://thispointer.com/how-to-create-a-directory-in-python/
if not os.path.exists("./stats/"):
    os.makedirs("./stats/")
file = open("./stats/samzaMetrics.csv", "w")
file.write("worker,avgProcessNs,avgWindowNs,avgProcessCallsPerSecondPerContainer,avgWindowCallsPerSecondPerContainer,sumProcessCallsPerSecondPerJob,sumWindowCallsPerSecondPerJob\n")

# https://realpython.com/iterate-through-dictionary-python/
for key in processNsMap:
    file.write(key + "," + processNsMap[key] + "," + windowNsMap[key] + "," + avgProcessCallsMap[key] + "," + avgWindowCallsMap[key] + "," + sumProcessCallsMap[key] + "," + sumWindowCallsMap[key] + "\n")

file.close()
