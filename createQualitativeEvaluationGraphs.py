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
from matplotlib.table import Table


# Table plot code heavily inspired by https://stackoverflow.com/a/10195347

def main():
    eventTypes = ['interceptionEvent', 'successfulPassEvent', 'freekickEvent', 'cornerkickEvent', 'throwinEvent', 'goalkickEvent']

    # https://stackoverflow.com/questions/47649284/half-space-in-matplotlib-labels-and-legend
    rowLabels = ['1$\,$s', '2$\,$s', '3$\,$s', '4$\,$s', '5$\,$s']
    columnLabels = ['1$\,$m', '3$\,$m', '5$\,$m', '7$\,$m', '9$\,$m', 'No distance\nthreshold']

    for eventType in eventTypes:
        # https://stackoverflow.com/questions/3518778/how-do-i-read-csv-data-into-a-record-array-in-numpy & https://docs.scipy.org/doc/numpy/reference/generated/numpy.genfromtxt.html
        csvContent = np.genfromtxt("./qualitativeEvalStats/" + eventType + "Stats.csv", delimiter=',', names=True)

        correctDetections = csvContent[::1]['correctDetectionsPercentage']
        correctDetectionsArray = correctDetections.reshape((5, 6))
        drawTable(correctDetectionsArray, rowLabels, columnLabels, 1, True)
        plt.savefig("./qualitativeEvalStats/" + eventType + "CorrectDetections.pdf")
        plt.clf()

        wrongDetections = csvContent[::1]['wrongDetectionsPercentage']
        wrongDetectionsArray = wrongDetections.reshape((5, 6))
        drawTable(wrongDetectionsArray, rowLabels, columnLabels, 1, False)
        plt.savefig("./qualitativeEvalStats/" + eventType + "WrongDetections.pdf")
        plt.clf()

        missedDetections = csvContent[::1]['missedDetectionsPercentage']
        missedDetectionsArray = missedDetections.reshape((5, 6))
        drawTable(missedDetectionsArray, rowLabels, columnLabels, 1, False)
        plt.savefig("./qualitativeEvalStats/" + eventType + "MissedDetections.pdf")
        plt.clf()


def drawTable(data, rowLabels, columnLabels, max, isPositive, fmt='{:.2f}'):
    fig, ax = plt.subplots()
    ax.set_axis_off()
    tb = Table(ax, bbox=[0, 0, 1, 1])

    nrows, ncols = data.shape
    width, height = 1.0 / ncols, 1.0 / nrows

    # Add cells
    for (i, j), val in np.ndenumerate(data):
        # https://matplotlib.org/2.0.2/api/colors_api.html
        if isPositive:
            color = (1 - (data[i][j] / max), 1, 1 - (data[i][j] / max))
        else:
            color = (1, 1 - (data[i][j] / max), 1 - (data[i][j] / max))

        tb.add_cell(i, j, width, height, text=fmt.format(val),
                    loc='center', facecolor=color)

        if j == 0:
            tb.add_cell(i, -1, width, height, text=rowLabels[i], loc='right',
                        edgecolor='white', facecolor='white')

        if i == 0:
            tb.add_cell(-1, j, width, height / 2, text=columnLabels[j], loc='center',
                        edgecolor='white', facecolor='white')

    tb.add_cell(-1, -1, width, height / 2, text="", loc='center',
                edgecolor='white', facecolor='white')

    ax.add_table(tb)
    return fig


if __name__ == '__main__':
    main()
