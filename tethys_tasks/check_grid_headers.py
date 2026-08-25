'''
Flags GRIB files whose grid header is self-inconsistent: the declared direction increment
disagrees with first/last/N.

Those files are the ones where eccodes' legacy geometry and eckit::geo produce different
coordinates, so the driver reading them must rebuild coordinates from the header (as
irm.py._read_local_helper does) rather than trusting ds.latitude. Getting that wrong is silent
until a join against older stored data fails with 'Latitudes do not match'.

    python -m tethys_tasks.check_grid_headers                 # every class with local files
    python -m tethys_tasks.check_grid_headers ALARO40L_T2M    # named classes only

Header-only, so it needs no stored file to compare against and decodes no data. Only the
latitude axis drives the verdict: eccodes reconciles an inconsistent j direction by dumping the
error into a trailing row while eckit-geo spreads it, whereas longitude agrees between the two.
Longitude is still reported, compared modulo 360 so global grids do not false-alarm.
'''

import sys
import tempfile
import zipfile
from pathlib import Path

import eccodes

import tethys_tasks
from tethys_tasks import BaseTask

GRIB_SUFFIXES = {'.grib', '.grib2', '.grb', '.grb2'}
TOL = 1e-9
FILES_PER_CLASS = 2


def _grib_source(path):
    '''A path to a GRIB message, extracted from a zip when needed. Returns (path, tempdir).'''

    path = Path(path)
    if path.suffix.lower() in GRIB_SUFFIXES:
        return path, None
    if path.suffix.lower() != '.zip':
        return None, None

    tmp = tempfile.TemporaryDirectory()
    with zipfile.ZipFile(path) as archive:
        names = [n for n in archive.namelist() if Path(n).suffix.lower() in GRIB_SUFFIXES]
        if not names:
            names = [n for n in archive.namelist() if 'grib' in n.lower() or 'grb' in n.lower()]
        if not names:
            tmp.cleanup()
            return None, None
        archive.extract(names[0], tmp.name)
        return Path(tmp.name) / names[0], tmp


def check_file(path):
    '''(verdict, detail) for one local file.'''

    source, tmp = _grib_source(path)
    try:
        if source is None:
            return 'NOTGRIB', ''
        with open(source, 'rb') as handle:
            message = eccodes.codes_grib_new_from_file(handle)
            if message is None:
                return 'UNREADABLE', ''
            try:
                grid = eccodes.codes_get(message, 'gridType')
                if grid != 'regular_ll':
                    return 'SKIP', f'gridType={grid}'
                get = lambda key: eccodes.codes_get(message, key)
                ni, nj = get('Ni'), get('Nj')
                lat1, lat2 = get('latitudeOfFirstGridPointInDegrees'), get('latitudeOfLastGridPointInDegrees')
                lon1, lon2 = get('longitudeOfFirstGridPointInDegrees'), get('longitudeOfLastGridPointInDegrees')
                jinc, iinc = get('jDirectionIncrementInDegrees'), get('iDirectionIncrementInDegrees')
            finally:
                eccodes.codes_release(message)
    finally:
        if tmp is not None:
            tmp.cleanup()

    lat_error = abs(abs(lat2 - lat1) - (nj - 1) * jinc)
    span, expected = (lon2 - lon1) % 360.0, (ni - 1) * iinc
    lon_error = min(abs(span - expected), abs(span + 360.0 - expected), abs(span - 360.0 - expected))
    verdict = 'INCONSISTENT' if lat_error > TOL else 'CONSISTENT'
    return verdict, 'lat err %.6f (%.3f..%.3f, Nj=%d, inc=%g)  lon err %.6f' % (
        lat_error, lat1, lat2, nj, jinc, lon_error)


def local_files(task):
    index = task.data_index
    if index is None or getattr(index, 'empty', True) or 'local_file' not in index.columns:
        return []
    found = []
    for name in index.sort_values('production_datetime', ascending=False)['local_file'].unique():
        if name and Path(str(name)).exists():
            found.append(Path(str(name)))
        if len(found) >= FILES_PER_CLASS:
            break
    return found


def main(argv):
    names = argv[1:]
    if not names:
        names = [n for n in sorted(tethys_tasks.__all__)
                 if isinstance(getattr(tethys_tasks, n, None), type)
                 and issubclass(getattr(tethys_tasks, n), BaseTask)
                 and getattr(tethys_tasks, n) is not BaseTask]

    inconsistent = []
    for name in names:
        cls = getattr(tethys_tasks, name, None)
        if cls is None:
            print('[%-12s] %-28s not exported' % ('MISSING', name))
            continue
        try:
            task = cls(verbose=0)
        except Exception as ex:
            print('[%-12s] %-28s init %s: %s' % ('ERROR', name, type(ex).__name__, ex))
            continue
        files = local_files(task)
        if not files:
            print('[%-12s] %-28s no local file on disk' % ('NODATA', name))
            continue
        for path in files:
            try:
                verdict, detail = check_file(path)
            except Exception as ex:
                verdict, detail = 'ERROR', f'{type(ex).__name__}: {ex}'
            if verdict == 'INCONSISTENT':
                inconsistent.append((name, path.name, detail))
            print('[%-12s] %-28s %-44s %s' % (verdict, name, path.name, detail))

    print('\n%d inconsistent-header file(s)' % len(inconsistent))
    for name, filename, detail in inconsistent:
        print('   %s  %s  %s' % (name, filename, detail))
    print('Drivers reading these must rebuild coordinates from the header keys; see irm.py.')
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
