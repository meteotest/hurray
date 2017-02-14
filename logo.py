"""
Create hurray logo
"""

import numpy as np
from scipy.ndimage import zoom
import matplotlib as mpl
from matplotlib import pyplot as plt
from matplotlib.figure import Figure
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas


def create_logo():
    data = np.array([
        [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        [0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0.5, 1, 1, 0, 0, 0, 0, 0, 1, 0, 0],
        [0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0],
        [0, 0, 1, 1, 1, 0, 1, 0, 1, 0, 1, 1, 1, 0, 1, 1, 1, 0, 1, 1, 1, 0, 1, 0, 1, 0, 1, 0, 0],
        [0, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 1, 0, 1, 0, 1, 0, 0, 0, 0],
        [0, 0, 1, 0, 1, 0, 1, 1, 1, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 1, 1, 0, 1, 1, 1, 0, 1, 0, 0],
        [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0],
        [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0],
        [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    ], dtype=np.float64)

    rand = 1 + (np.random.random(data.shape) - 0.5) * 0.1
    data = data * rand

    data = zoom(data, zoom=1.6, order=5, mode="constant")
    cmap = "YlGnBu"

    # big logo
    width, height = 300, 110
    DPI = 100
    width_inch, height_inch = width / DPI, height / DPI
    fig = Figure(frameon=False)
    fig.set_size_inches((width_inch, height_inch))
    FigureCanvas(fig)
    ax = fig.add_axes([0., 0., 1., 1.])
    ax.set_axis_off()
    ax.pcolor(data[::-1], edgecolors='#777777', linewidths=0.6)
    # extent = (ax.get_window_extent().
    #           transformed(fig.dpi_scale_trans.inverted()))
    extent = mpl.transforms.Bbox(((0, 0), (width_inch, height_inch)))
    fig.savefig("logo.png", dpi=DPI, transparent=True, bbox_inches=extent,
                format="png")
    fig.clf()  # important to release memory!

    # create a small logo consisting of only the "h"
    width, height = 95, 150 
    width_inch, height_inch = width / DPI, height / DPI
    fig = Figure(frameon=False)
    fig.set_size_inches((width_inch, height_inch))
    FigureCanvas(fig)
    ax = fig.add_axes([0., 0., 1., 1.])
    ax.set_axis_off()
    ax.pcolor(data[::-1][1:, 1:9], edgecolors='#888888', linewidths=0.4)
    extent = mpl.transforms.Bbox(((0, 0), (width_inch, height_inch)))
    fig.savefig("logo_small.png", dpi=DPI, transparent=True, bbox_inches=extent,
                format="png")
    fig.clf()  # important to release memory!


if __name__ == "__main__":
    create_logo()
