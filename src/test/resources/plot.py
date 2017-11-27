import sys
import csv
import math
import matplotlib.pyplot as plt


def customized_box_plot(percentiles, axes, redraw=True, *args, **kwargs):
    """
    Generates a customized boxplot based on the given percentile values
    """

    n_box = len(percentiles)
    boxplot = axes.boxplot([[-9, -4, 2, 4, 9], ] * n_box, *args, **kwargs)
    y_min = None
    y_max = None

    for box_no, (label, q1_start, q2_start, q3_start, q4_start, q4_end) in enumerate(percentiles):
        boxplot['caps'][2 * box_no].set_ydata([q1_start, q1_start])
        boxplot['whiskers'][2 * box_no].set_ydata([q1_start, q2_start])
        boxplot['caps'][2 * box_no + 1].set_ydata([q4_end, q4_end])
        boxplot['whiskers'][2 * box_no + 1].set_ydata([q4_start, q4_end])
        boxplot['boxes'][box_no].set_ydata([q2_start, q2_start, q4_start, q4_start, q2_start])
        boxplot['medians'][box_no].set_ydata([q3_start, q3_start])

        if y_min is None or y_min > q2_start:
            y_min = q2_start

        if y_max is None or y_max < q4_start:
            y_max = q4_start

    # axes.set_yscale('log')
    axes.set_ylim(ymin=math.floor(y_min), ymax=math.ceil(y_max + 1))

    # If redraw is set to true, the canvas is updated.
    if redraw:
        ax.figure.canvas.draw()

    plt.xticks(range(1, n_box + 1), map(lambda x: x[0], percentiles))

    return boxplot


csvfilename = sys.argv[1]
fig, ax = plt.subplots()
plt.title(csvfilename)
plt.ylabel('Latency in Milliseconds')
plt.xlabel('Test Run')
with open(csvfilename, 'r') as csvfile:
    percentiles = []
    csvreader = csv.reader(csvfile, delimiter=',', quotechar='\'')
    for row in csvreader:
        percentiles.append((row[0], float(row[1]), float(row[2]), float(row[3]), float(row[4]), float(row[5])))
    b = customized_box_plot(percentiles, ax, redraw=True, notch=0, sym='+', vert=1, whis=1.5)
    plt.show()
