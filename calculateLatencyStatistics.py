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
import os

streamNames = ['ballObjectState', 'A1FullGameHeatmapStatistics', 'kickEvent', 'BPassStatistics', 'passSequenceEvent']

# https://stackoverflow.com/questions/8703496/hash-map-in-python
F2ALL = {}
X2ALL = {}

maxPercentile99 = 0

# https://community.tableau.com/thread/121769 -> colors: ['#1F77B4', '#FF7F0E', '#2CA02C', '#D62728', '#9467BD', '#8C564B', '#CFECF9', '#7F7F7F', '#BCBD22', '#17BECF']
colors = {'ballObjectState': '#1F77B4', 'A1FullGameHeatmapStatistics': '#FF7F0E', 'kickEvent': '#2CA02C', 'BPassStatistics': '#D62728', 'passSequenceEvent': '#9467BD'}

# https://www.pythonforbeginners.com/files/reading-and-writing-files-in-python & https://thispointer.com/how-to-create-a-directory-in-python/
if not os.path.exists("./stats/"):
    os.makedirs("./stats/")
file = open("./stats/latencyStats.csv", "w")
file.write("streamName,mean,std,var,median,90thPercentile,99thPercentile\n")

for streamName in streamNames:
    # https://stackoverflow.com/questions/3518778/how-do-i-read-csv-data-into-a-record-array-in-numpy & https://docs.scipy.org/doc/numpy/reference/generated/numpy.genfromtxt.html
    csvContent = np.genfromtxt("./latencies/" + streamName + 'Latencies.csv', delimiter=',', names=True)
    latencies = csvContent[::1]['latencyInMs']

    # https://docs.scipy.org/doc/numpy/reference/generated/numpy.median.html
    mean = np.mean(latencies)
    std = np.std(latencies)
    var = np.var(latencies)
    median = np.median(latencies)
    percentile90 = np.percentile(latencies, 90)
    percentile99 = np.percentile(latencies, 99)
    file.write(streamName + "," + str(mean) + "," + str(std) + "," + str(var) + "," + str(median) + "," + str(percentile90) + "," + str(percentile99) + "\n")

    # https://stackoverflow.com/questions/10640759/how-to-get-the-cumulative-distribution-function-with-numpy & https://futurestud.io/tutorials/matplotlib-save-plots-as-file & https://matplotlib.org/3.1.1/api/_as_gen/matplotlib.pyplot.xlim.html
    F2 = np.array(range(latencies.size)) / float(latencies.size)
    X2 = np.sort(latencies)
    plt.xlim(0, percentile99)
    plt.grid(True)
    plt.xlabel('Latency in ms')
    # https://stackoverflow.com/questions/18428823/python-matplotlib-less-than-or-equal-to-symbol-in-text
    plt.ylabel('P(Latency $\leq$ x)')
    plt.plot(X2, F2, color=colors[streamName])
    plt.savefig("./stats/" + streamName + 'LatencyCDF.pdf')
    plt.clf()

    F2ALL[streamName] = F2
    X2ALL[streamName] = X2
    maxPercentile99 = max(maxPercentile99, percentile99)

file.close()

plt.xlim(0, maxPercentile99)
plt.grid(True)
plt.xlabel('Latency in ms')
plt.ylabel('P(Latency $\leq$ x)')
for streamName in streamNames:
    plt.plot(X2ALL[streamName], F2ALL[streamName], label=streamName, color=colors[streamName])
# https://matplotlib.org/3.1.1/api/_as_gen/matplotlib.pyplot.legend.html
plt.legend(loc='lower right', fontsize='x-small')
plt.savefig('./stats/AllLatencyCDFs.pdf')
plt.clf()
