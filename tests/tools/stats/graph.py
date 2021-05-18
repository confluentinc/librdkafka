#!/usr/bin/env python3
#
# Use pandas + bokeh to create graphs/charts/plots for stats CSV (to_csv.py).
#

import os
import pandas as pd
import numpy
from bokeh.io import curdoc
from bokeh.models import ColumnDataSource, HoverTool
from bokeh.plotting import figure
from bokeh.palettes import Dark2_5 as palette
from bokeh.models.formatters import DatetimeTickFormatter, PrintfTickFormatter

import pandas_bokeh
import argparse
from datetime import datetime
import itertools
from fnmatch import fnmatch

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Graph CSV files')
    parser.add_argument('infiles', nargs='+', type=str,
                        help='CSV files to plot.')
    parser.add_argument('--cols', type=str,
                        help='Columns to plot (CSV list)')
    parser.add_argument('--skip', type=str,
                        help='Columns to skip (CSV list)')
    parser.add_argument('--group-by', type=str,
                        help='Group data series by field')
    parser.add_argument('--chart-cols', type=int, default=3,
                        help='Number of chart columns')
    parser.add_argument('--plot-width', type=int, default=400,
                        help='Per-plot width')
    parser.add_argument('--plot-height', type=int, default=300,
                        help='Per-plot height')
    parser.add_argument('--date-col', type=str, default='0time',
                        help='Column name for the series datetime (x axis)')
    parser.add_argument('--out', type=str, default='out.html',
                        help='Output file (HTML)')
    parser.add_argument('--title', type=str, default=None,
                        help='Title to add to graphs')
    args = parser.parse_args()

    datecolumn = args.date_col
    outpath = args.out
    if args.title is not None:
        add_title = f"{args.title}: "
    else:
        add_title = ""

    if args.cols is None:
        cols = None
    else:
        cols = args.cols.split(',')
        cols.append(datecolumn)

    if args.skip is not None:
        assert cols is None, "--cols and --skip are mutually exclusive"
        skip = args.skip.split(',')
    else:
        skip = None

    group_by = args.group_by

    pandas_bokeh.output_file(outpath)
    curdoc().theme = 'dark_minimal'

    figs = {}
    plots = []
    for infile in args.infiles:

        colors = itertools.cycle(palette)

        cols_to_use = cols

        # Sample two rows of the CSV file to get the header
        # and a single data entry line.
        sdf = pd.read_csv(infile, nrows=2, skipinitialspace=True)

        if len(sdf) < 2:
            raise Exception(f"input file {infile} seems to be empty")

        # Check the format of the date column
        sdate = sdf[datecolumn].iloc[1]
        if isinstance(sdate, numpy.int64):
            date_parser = lambda col: datetime.fromtimestamp(int(col)/1000)
        else:
            date_parser = None

        if skip is not None:
            avail_cols = list(sdf)
            cols_to_use = [c for c in avail_cols
                           if len([x for x in skip
                                   if fnmatch(c.strip().lower(),
                                              x.strip().lower())]) == 0]

        if group_by is not None and group_by not in cols_to_use:
            cols_to_use.append(group_by)

        df = pd.read_csv(infile,
                         parse_dates=[datecolumn],
                         date_parser=date_parser,
                         index_col=datecolumn,
                         usecols=cols_to_use,
                         skipinitialspace=True)
        title = add_title + os.path.basename(infile)
        print(f"{infile}:")

        if group_by is not None:

            grp = df.groupby([group_by])

            # Make one plot per column, skipping the index and group_by cols.
            for col in df.keys():
                if col in (datecolumn, group_by):
                    continue

                print("col: ", col)

                for _, dg in grp:
                    print(col, " dg:\n", dg.head())
                    figtitle = f"{title}: {col}"
                    p = figs.get(figtitle, None)
                    if p is None:
                        p = figure(title=f"{title}: {col}",
                                   plot_width=args.plot_width,
                                   plot_height=args.plot_height,
                                   x_axis_type='datetime',
                                   tools="hover,box_zoom,wheel_zoom," +
                                   "reset,pan,poly_select,tap,save")
                        figs[figtitle] = p
                        plots.append(p)

                        p.add_tools(HoverTool(
                            tooltips=[
                                ("index", "$index"),
                                ("time", f"@{datecolumn}{{%F}}"),
                                ("y", "$y"),
                                ("desc", "$name"),
                            ],
                            formatters={
                                f"@{datecolumn}": 'datetime',
                            },
                            mode='vline'))

                        p.xaxis.formatter = DatetimeTickFormatter(
                            minutes=['%H:%M'],
                            seconds=['%H:%M:%S'])

                    source = ColumnDataSource(dg)

                    val = dg[group_by][0]
                    for k in dg:
                        if k != col:
                            continue

                        p.line(x=datecolumn, y=k, source=source,
                               legend_label=f"{k}[{val}]",
                               name=f"{k}[{val}]",
                               color=next(colors))

            continue

        else:
            p = df.plot_bokeh(title=title,
                              kind='line', show_figure=False)

        plots.append(p)

    for p in plots:
        p.legend.click_policy = "hide"

    grid = []
    for i in range(0, len(plots), args.chart_cols):
        grid.append(plots[i:i+args.chart_cols])

    pandas_bokeh.plot_grid(grid)
