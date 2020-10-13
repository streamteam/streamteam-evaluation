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

workerNames = ['AreaDetectionApplication', 'DistanceAndSpeedAnalysisApplication', 'HeatmapApplication', 'KickoffDetectionApplication', 'PressingAnalysisApplication', 'TeamAreaApplication', 'BallPossessionApplication', 'KickDetectionApplication', 'OffsideApplication', 'SetPlayDetectionApplication', 'TimeApplication']
# Not PassAndShotDetectionApplication, FieldObjectStateGenerationApplication and PassCombinationDetectionApplication since they do not process field object state stream elements

# https://stackoverflow.com/questions/8703496/hash-map-in-python
processingTimestamps = {}

i = 0
for workerName in workerNames:
    # https://www.pythonforbeginners.com/files/reading-and-writing-files-in-python
    logFile = open("./workerLogs/" + workerName + ".log", "r")

    for line in logFile.readlines():
        # https://www.tutorialspoint.com/python/string_startswith.htm
        if line.startswith("ProcessingTimestampOfTheFieldObjectStateStreamElementForTheBall"):
            lineParts = line.split(" ")
            # https://www.w3schools.com/python/ref_string_zfill.asp
            key = lineParts[1].zfill(10) + "-" + lineParts[2].zfill(10)  # zfill for later proper sort
            processingTimestamp = float(lineParts[3])

            if i > 0:
                # https://www.geeksforgeeks.org/append-extend-python/
                processingTimestamps[key].extend([processingTimestamp])
            else:
                processingTimestamps[key] = [processingTimestamp]

    print "Parsed ./workerLogs/" + workerName + ".log"
    i = i + 1

print "Parsed all log files"

# https://www.pythonforbeginners.com/files/reading-and-writing-files-in-python & https://thispointer.com/how-to-create-a-directory-in-python/
if not os.path.exists("./processingTimestampVarianceStats/"):
    os.makedirs("./processingTimestampVarianceStats/")
variancesFile = open("./processingTimestampVarianceStats/processingTimestampVariances.csv", "w")
variancesFile.write("matchId,generationTimestamp,var,std,num\n")

vars = []
stds = []
nums = []
generationTimestamps = []

sortedKeys = np.sort(processingTimestamps.keys())
for key in sortedKeys:
    keyParts = key.split("-")
    matchId = int(keyParts[0])
    generationTimestamp = int(keyParts[1])

    var = np.var(processingTimestamps[key])
    std = np.std(processingTimestamps[key])
    num = len(processingTimestamps[key])

    variancesFile.write(str(matchId) + "," + str(generationTimestamp) + "," + str(var) + "," + str(std) + "," + str(num) + "\n")

    # https://www.geeksforgeeks.org/append-extend-python/
    vars.extend([var])
    stds.extend([std])
    nums.extend([num])
    generationTimestamps.extend([generationTimestamp])

variancesFile.close()
print "Wrote ./processingTimestampVarianceStats/processingTimestampVariances.csv"

meanVar = np.mean(vars)
print "Mean variance: " + str(meanVar)
meanStd = np.mean(stds)
print "Mean standard deviation: " + str(meanStd)
meanNum = np.mean(num)
print "Mean num: " + str(meanNum)

statsFile = open("./processingTimestampVarianceStats/stats.csv", "w")
statsFile.write("meanVar,meanStd,meanNum\n")
statsFile.write(str(meanVar) + "," + str(meanStd) + "," + str(meanNum) + "\n")
statsFile.close()
print "Wrote ./processingTimestampVarianceStats/stats.csv"

minGenerationTimestamp = np.min(generationTimestamps)
maxGenerationTimestamp = np.max(generationTimestamps)

# https://jakevdp.github.io/PythonDataScienceHandbook/04.05-histograms-and-binnings.html & https://matplotlib.org/3.1.1/api/_as_gen/matplotlib.axes.Axes.hist2d.html & https://futurestud.io/tutorials/matplotlib-save-plots-as-file
plt.hist2d(generationTimestamps, vars, bins=[50, 50], cmap='Blues', range=[[minGenerationTimestamp, maxGenerationTimestamp], [0, np.percentile(vars, 99)]])
plt.ylabel('Processing timestamp variance in ms')
plt.xlabel('Match time')
plt.xticks([minGenerationTimestamp, maxGenerationTimestamp], ['Start', 'End'])
cb = plt.colorbar()
cb.set_label('Counts in bin')
plt.savefig("./processingTimestampVarianceStats/processingTimestampVariancesOverTime.pdf")
plt.clf()
print "Created ./processingTimestampVarianceStats/processingTimestampVariancesOverTime.pdf"

plt.hist2d(generationTimestamps, stds, bins=[50, 50], cmap='Blues', range=[[minGenerationTimestamp, maxGenerationTimestamp], [0, np.percentile(stds, 99)]])
plt.ylabel('Processing timestamp standard deviation in ms')
plt.xlabel('Match time')
plt.xticks([minGenerationTimestamp, maxGenerationTimestamp], ['Start', 'End'])
cb = plt.colorbar()
cb.set_label('Counts in bin')
plt.savefig("./processingTimestampVarianceStats/processingTimestampStandardDeviationsOverTime.pdf")
plt.clf()
print "Created ./processingTimestampVarianceStats/processingTimestampStandardDeviationsOverTime.pdf"
