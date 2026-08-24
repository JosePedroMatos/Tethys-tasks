'''
Build-time smoke test for the GRIB stack. Exits non-zero on failure.

    python -m tethys_tasks.check_grib_stack            # runs both modes, one subprocess each
    python -m tethys_tasks.check_grib_stack stock      # single mode, in this process

Lives inside the package because .dockerignore excludes tests/ from the image.

Guards the failure mode of 2026-08-24: icon_ch set ECCODES_DEFINITION_PATH process-wide to a
vendored eccodes ~2.38 overlay, so when a rebuild moved eccodes to 2.47 every GRIB driver
stopped decoding.

The two modes run in separate processes on purpose. eccodes caches its definitions on first
use, so whichever set is used first wins for the rest of the process: decode COSMO first and a
stock sample then reads shortName 'T_G' instead of 't'. That is why each Airflow task runs a
single class per container, and why icon_ch.read_local can rely on COSMO being active.

The env legitimately carries two eccodes libraries (conda eccodes/python-eccodes and the pip
eccodeslib that earthkit-data hard-pins). That is fine as long as the library and the
definitions actually loaded agree, which is what decoding proves.
'''

import os
import subprocess
import sys
from pathlib import Path


EXPECTED_API = '2.47'
EXPECTED_SAMPLE = ('t', 31)
MODES = ('stock', 'cosmo')


def _common_problems(tethys_tasks, eccodes):
    problems = []

    api = eccodes.codes_get_api_version()
    if not api.startswith(EXPECTED_API):
        problems.append(f'eccodes api version {api}, expected {EXPECTED_API}.*')

    repo_resources = str(Path(tethys_tasks.__file__).resolve().parent / 'resources')
    for part in eccodes.codes_definition_path().split(':'):
        if part.startswith(repo_resources):
            problems.append(f'definitions path points into the repository ({part}); COSMO tables '
                            'must stay scoped to meteodatalab.data_source.cosmo_grib_defs')

    return problems


def check_geometry(tethys_tasks, eccodes):
    '''
    eccodes' grid-geometry backend. earthkit-data sets ECCODES_ECKIT_GEO=1 at import and
    ICON-CH/icon_world need it: eckit::geo is what supports unstructured grids. Forcing it off
    (to get the legacy regular_ll reconciliation) breaks those readers, so guard it here --
    irm.py rebuilds its own coordinates from the header instead of relying on the backend.
    '''

    value = os.environ.get('ECCODES_ECKIT_GEO')
    if value == '0':
        return ['ECCODES_ECKIT_GEO is "0"; eckit-geo is off, so unstructured grids '
                '(ICON-CH, ICON_WORLD) cannot be read'], None
    return [], f'eckit-geo enabled (ECCODES_ECKIT_GEO={value!r})'


def check_stock(tethys_tasks, eccodes):
    '''The path every driver except ICON-CH decodes through.'''

    try:
        handle = eccodes.codes_grib_new_from_samples('regular_ll_sfc_grib2')
        try:
            sample = eccodes.codes_get(handle, 'shortName'), eccodes.codes_get_size(handle, 'distinctLatitudes')
        finally:
            eccodes.codes_release(handle)
    except Exception as ex:
        sample = f'{type(ex).__name__}: {ex}'

    if sample != EXPECTED_SAMPLE:
        return [f'stock sample decoded as {sample!r}, expected {EXPECTED_SAMPLE!r}'], None
    return [], f'stock definitions {eccodes.codes_definition_path()}'


def check_cosmo(tethys_tasks, eccodes):
    '''The ICON-CH path, on a real DWD-encoded file shipped with the package.'''

    import eccodes_cosmo_resources
    from meteodatalab import data_source, grib_decoder
    from meteodatalab.data_source import cosmo_grib_defs

    constants = sorted((Path(tethys_tasks.__file__).resolve().parent / 'resources' / 'icon_ch')
                       .glob('horizontal_constants_*.grib2'))
    if not constants:
        return ['no ICON-CH horizontal constants shipped; ICON-CH cannot be verified'], None

    problems = []
    expected = str(eccodes_cosmo_resources.get_definitions_path())
    with cosmo_grib_defs():
        active = eccodes.codes_definition_path()
    if expected not in active:
        problems.append(f'cosmo_grib_defs did not activate {expected} ({active})')

    # A stale or absent COSMO overlay makes load() return {} rather than raise, which is how
    # this failure stayed silent until read_local hit KeyError: 'CLON'.
    source = data_source.FileDataSource(datafiles=[str(constants[0])])
    fields = grib_decoder.load(source, {'param': ['CLON', 'CLAT']}, geo_coords=lambda _: {})
    missing = [param for param in ('CLON', 'CLAT') if param not in fields]
    if missing:
        problems.append(f'COSMO tables do not resolve {missing} in {constants[0].name} '
                        f'(got {sorted(fields)}); ICON-CH cannot read')

    return problems, f'cosmo definitions {active}'


def run_mode(mode):
    # Importing tethys_tasks first is the point: a module-level ECCODES_DEFINITION_PATH override
    # in any driver has to show up below. Importing does not itself decode GRIB.
    import tethys_tasks
    import eccodes

    problems = _common_problems(tethys_tasks, eccodes)
    note = None
    try:
        problems_, note = {'stock': check_stock, 'cosmo': check_cosmo}[mode](tethys_tasks, eccodes)
        problems += problems_
        if mode == 'stock':
            geometry_problems, geometry_note = check_geometry(tethys_tasks, eccodes)
            problems += geometry_problems
            if geometry_note:
                note = f'{note}; {geometry_note}'
    except Exception as ex:
        problems.append(f'{mode} definitions unusable: {type(ex).__name__}: {ex}')

    if problems:
        print(f'GRIB stack check FAILED [{mode}] (eccodes {eccodes.codes_get_api_version()}):')
        for problem in problems:
            print(f'  - {problem}')
        return 1

    print(f'GRIB stack OK [{mode}]: {note}')
    return 0


def main(argv):
    if len(argv) > 1:
        if argv[1] not in MODES:
            print(f'usage: python -m tethys_tasks.check_grib_stack [{"|".join(MODES)}]')
            return 2
        return run_mode(argv[1])

    # One subprocess per mode: see the module docstring on eccodes definition caching.
    failed = 0
    for mode in MODES:
        result = subprocess.run([sys.executable, '-m', 'tethys_tasks.check_grib_stack', mode])
        failed |= result.returncode
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
