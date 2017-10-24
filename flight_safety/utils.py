"""
Util functions
"""

import numpy as np


def convert_lat(string):
    try:
        degs = float(string[0:2])
        mins = float(string[2:4])
        secs = float(string[4:6])
        last = string[6].lower()
        if last == 's':
            factor = -1.0
        elif last == 'n':
            factor = 1.0
        else:
            raise ValueError("invalid hemisphere")
        return factor * (degs + mins / 60 + secs / 3600)
    except ValueError:
        return np.nan


def convert_lon(string):
    try:
        degs = float(string[0:3])
        mins = float(string[3:5])
        secs = float(string[5:7])
        last = string[7].lower()
        if last == 'w':
            factor = -1.0
        elif last == 'e':
            factor = 1.0
        else:
            raise ValueError("invalid direction")
        return factor * (degs + mins / 60 + secs / 3600)
    except ValueError:
        return np.nan


def rename_categories(old_categories, codes_meaning):
    new_categories = []
    for cat in old_categories:
        if str(int(cat)) in codes_meaning.index:
            new_categories.append(codes_meaning.loc[str(int(cat)), 'meaning'])
        else:
            new_categories.append(cat)
    return new_categories
