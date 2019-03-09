"""
Module to do MIG filename parsing and regex matching
"""

import glob
import re
from typing import Optional, Iterator

NPS_PATTERN = '(?P<path>(?:.*\/)?(?P<filename>(?P<sender>[0-9]{13})\.(?P<receiver>[0-9]{13})\.(?P<sequence>[0-9]*)\.(?P<export>EXPORT(?P<export_no>[0-9]{2})[^\.]*)\.(?P<mig>MIG[^\.]*)\.csv))'

def match_filename(filename: str) -> Optional[dict]:
    """
    Match the filename to the NPS Pattern

    Parameters
    ----------
    filename : str

    Returns
    -------
    dict
        A dict with the full file path, filename, and components
        If no match is found, None is returned
    """
    r = re.match(string=filename, pattern=NPS_PATTERN, flags=re.I)
    if r:
        return r.groupdict()
    else:
        return None


def find_files(pathname: str) -> Iterator[dict]:
    """
    Finds all files that match the NPS Pattern in a given directory

    Parameters
    ----------
    pathname : str
        directory to search in

    Yields
    -------
    dict
    """
    for filename in glob.iglob(f'{pathname}/*'):
        r = match_filename(filename=filename)
        if r:
            yield r