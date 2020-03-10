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

import matplotlib.pyplot as plt
import numpy as np
import sys

if len(sys.argv) != 2:
    sys.exit("Required parameters: <maxNumberOfSimulations>")

maxNumberOfSimulations = int(sys.argv[1])

# https://community.tableau.com/thread/121769 -> colors: ['#1F77B4', '#FF7F0E', '#2CA02C', '#D62728', '#9467BD', '#8C564B', '#CFECF9', '#7F7F7F', '#BCBD22', '#17BECF']
streamColors = {'ballObjectState': '#1F77B4', 'A1FullGameHeatmapStatistics': '#FF7F0E', 'kickEvent': '#2CA02C', 'BPassStatistics': '#D62728', 'passSequenceEvent': '#9467BD'}
# http://tableaufriction.blogspot.com/2012/11/finally-you-can-use-tableau-data-colors.html  -> colors: ['#1F77B4', '#AEC7E8', '#FF7F0E', '#FFBB78', '#2CA02C', '#98DF8A', '#D62728', '#98DF8A', '#9467BD', '#98DF8A', '#8C564B', '#C49C94', '#CFECF9', '#F7B6D2', '#7F7F7F', '#F7B6D2', '#BCBD22', '#DBDB8D', '#17BECF', '#9EDAE5']
workerColors = {'TimeTask': '#1F77B4', 'FieldObjectStateGenerationTask': '#AEC7E8', 'DistanceAndSpeedAnalysisTask': '#FF7F0E', 'PassCombinationDetectionTask': '#FFBB78', 'PressingAnalysisTask': '#2CA02C', 'HeatmapTask': '#98DF8A', 'KickDetectionTask': '#D62728', 'TeamAreaTask': '#98DF8A', 'PassAndShotDetectionTask': '#9467BD', 'SetPlayDetectionTask': '#98DF8A', 'AreaDetectionTask': '#8C564B', 'KickoffDetectionTask': '#C49C94', 'OffsideTask': '#CFECF9', 'BallPossessionTask': '#F7B6D2'}

# https://stackoverflow.com/questions/8703496/hash-map-in-python
values = {}

streamNames = []
workers = []

latencyMeasures = ['mean', 'std', 'var', 'median', '90thPercentile', '99thPercentile']
latencyMeasureLabels = {'mean': 'Mean latency in ms', 'std': 'Latency standard deviation in ms', 'var': 'Latency variance in ms', 'median': 'Median latency in ms', '90thPercentile': '90th percentile latency in ms', '99thPercentile': '99th percentile latency in ms'}

samzaMetrics = ['avgProcessNs', 'avgWindowNs', 'avgProcessCallsPerSecondPerContainer', 'avgWindowCallsPerSecondPerContainer', 'sumProcessCallsPerSecondPerJob', 'sumWindowCallsPerSecondPerJob']
samzaMetricLabels = {'avgProcessNs': 'Average duration of a process call in ns', 'avgWindowNs': 'Average duration of a window call in ns', 'avgProcessCallsPerSecondPerContainer': 'Average number of process calls per s per container', 'avgWindowCallsPerSecondPerContainer': 'Average number of window calls per s per container', 'sumProcessCallsPerSecondPerJob': 'Sum of process calls per s per job', 'sumWindowCallsPerSecondPerJob': 'Sum of window calls per s per job'}

# https://www.w3schools.com/python/python_for_loops.asp
for i in range(maxNumberOfSimulations):
    csvContent = np.genfromtxt('./evaluationRowOutput/' + str(i + 1) + '/stats/latencyStats.csv', delimiter=',', names=True, dtype=None)
    if i == 0:
        streamNames = csvContent[::1]['streamName']
    for j in range(csvContent.size):
        for latencyMeasure in latencyMeasures:
            if i == 0 and j == 0:
                values[latencyMeasure] = {}
            if i == 0:
                values[latencyMeasure][csvContent[j]['streamName']] = [csvContent[j][latencyMeasure]]
            else:
                # https://www.geeksforgeeks.org/append-extend-python/
                values[latencyMeasure][csvContent[j]['streamName']].extend([csvContent[j][latencyMeasure]])

    csvContent = np.genfromtxt('./evaluationRowOutput/' + str(i + 1) + '/stats/samzaMetrics.csv', delimiter=',', names=True, dtype=None)
    if i == 0:
        workers = csvContent[::1]['worker']
    for j in range(csvContent.size):
        for samzaMetric in samzaMetrics:
            if i == 0 and j == 0:
                values[samzaMetric] = {}
            if i == 0:
                values[samzaMetric][csvContent[j]['worker']] = [csvContent[j][samzaMetric]]
            else:
                # https://www.geeksforgeeks.org/append-extend-python/
                values[samzaMetric][csvContent[j]['worker']].extend([csvContent[j][samzaMetric]])

for latencyMeasure in latencyMeasures:
    file = open("./evaluationRowOutput/" + latencyMeasure + "Latencies.csv", "w")
    file.write("streamName")
    for i in range(maxNumberOfSimulations):
        file.write(',')
        file.write(str(i + 1))
    file.write('\n')

    for streamName in streamNames:
        file.write(streamName)
        for i in range(maxNumberOfSimulations):
            file.write(',')
            file.write(str(values[latencyMeasure][streamName][i]))
        file.write('\n')

    file.close()

    # https://futurestud.io/tutorials/matplotlib-save-plots-as-file & https://matplotlib.org/3.1.1/api/_as_gen/matplotlib.pyplot.xlim.html & https://matplotlib.org/2.1.0/api/_as_gen/matplotlib.pyplot.xticks.html & https://matplotlib.org/3.1.1/api/_as_gen/matplotlib.pyplot.legend.html
    plt.xticks(np.arange(1, maxNumberOfSimulations + 1))
    plt.xlim(1, maxNumberOfSimulations)
    plt.grid(True)
    plt.xlabel('Number of concurrently simulated matches')
    plt.ylabel(latencyMeasureLabels[latencyMeasure])
    for streamName in streamNames:
        plt.plot(np.array(range(1, maxNumberOfSimulations + 1)), values[latencyMeasure][streamName], label=streamName, color=streamColors[streamName])
    plt.legend(loc='best', fontsize='x-small')
    plt.savefig("./evaluationRowOutput/" + latencyMeasure + "Latencies.pdf")
    plt.clf()

for samzaMetric in samzaMetrics:
    file = open("./evaluationRowOutput/" + samzaMetric + ".csv", "w")
    file.write("worker")
    for i in range(maxNumberOfSimulations):
        file.write(',')
        file.write(str(i + 1))
    file.write('\n')

    maxVal = 0
    for worker in workers:
        file.write(worker)
        for i in range(maxNumberOfSimulations):
            file.write(',')
            file.write(str(values[samzaMetric][worker][i]))
            maxVal = max(maxVal, values[samzaMetric][worker][i])
        file.write('\n')

    file.close()

    # https://futurestud.io/tutorials/matplotlib-save-plots-as-file & https://matplotlib.org/3.1.1/api/_as_gen/matplotlib.pyplot.xlim.html & https://matplotlib.org/2.1.0/api/_as_gen/matplotlib.pyplot.xticks.html
    plt.xticks(np.arange(1, maxNumberOfSimulations + 1))
    plt.xlim(1, maxNumberOfSimulations)
    if samzaMetric == 'avgWindowCallsPerSecondPerContainer' or samzaMetric == 'sumWindowCallsPerSecondPerJob':
        plt.ylim(0, maxVal + 1)
    plt.grid(True)
    plt.xlabel('Number of concurrently simulated matches')
    plt.ylabel(samzaMetricLabels[samzaMetric])
    for worker in workers:
        plt.plot(np.array(range(1, maxNumberOfSimulations + 1)), values[samzaMetric][worker], label=worker, color=workerColors[worker])
    plt.legend(loc='best', fontsize='x-small')
    plt.savefig("./evaluationRowOutput/" + samzaMetric + ".pdf")
    plt.clf()
