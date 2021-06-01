# This dataset procedure configures a dataset for use as a KDENLIVE project
# Most importantly it configures Git to not commit the absolute root path
# configured in a project file into Git. Instead a placeholder is stored
# that is replaced with the absolute path of the dataset root location
# on checkout. This makes KDENLIVE project datasets portable.
#
# Moreover, it:
# - expects the project file to be named 'project.kdenlive', and to be
#   placed in the root of the dataset
# - creates a standard directory structure
# - configures Git to ignore a number of temporary working directories
#   that KDENLIVE might create
#
# The dataset procedure is idempotent, and is save to run on
# datalad-create (with -c), and must be executed once in any clone
# of such a dataset to apply all necessary configuration.

import sys
from pathlib import Path
from datalad.api import Dataset

ds = Dataset(sys.argv[1])

gitignore = ds.pathobj / '.gitignore'
gitignore_content = gitignore.open().read().splitlines() \
    if gitignore.exists() else []

to_save = set()

# directories that KDEnlive will automatically create
# ignore them
# (cache dir names are all numerical)
for wdir in (
        'proxy',
        'thumbs',
        'titles',
        '.backup',
        '[0-9][0-9][0-9]*/',
        'project_backup*.kdenlive'):
    if wdir not in gitignore_content:
        gitignore_existed = gitignore.exists()
        gitignore.open('a').write('{}{}'.format(
            '\n' if gitignore_existed else '',
            wdir))
        to_save.add(gitignore.name)

# add a default directory for materials (video, audio, images)
# that a project is comprised of. we do not distinguish
# between type of material here (unlike the manual suggests),
# because there are other way to organize, and it violates
# the principles of modularity (one project being material
# for another)
resource_dir = ds.pathobj / 'materials'
resource_dir.mkdir(exist_ok=True)

projectfile = 'project.kdenlive'
filtername = 'kdenlive-project-root'

ds_repo = ds.repo

# we must configure the filter in the non-committed attributes
# because git-annex does it there, and we have no chance to override
# it elsewhere
if ds_repo.get_gitattributes(projectfile).get(
        projectfile, {}).get('filter', None) != filtername:
    ds_repo.set_gitattributes(
        [(projectfile, {'filter': filtername})],
        attrfile=Path(ds_repo.get_git_dir(ds_repo)) / 'info' / 'attributes',
        mode='a',
    )
if ds_repo.get_gitattributes(projectfile).get(
        projectfile, {}).get('annex.largefiles', None) != 'nothing':
    ds_repo.set_gitattributes(
        [(projectfile, {'annex.largefiles': 'nothing'})],
        attrfile='.gitattributes',
        mode='a',
    )
    to_save.add('.gitattributes')

ds_repo.config.set(
    'filter.{}.clean'.format(filtername),
    'sed \'0,/^<mlt/s/^\(<mlt.*root="\)\(.*\)\(".*\)$/\\1{DATALADPROJECTROOT}\\3/\'',
    where='local',
    force=True,
    reload=False,
)
ds_repo.config.set(
    'filter.{}.smudge'.format(filtername),
    'sed \'0,/^<mlt/s,^\(<mlt.*root="\){DATALADPROJECTROOT}\(".*\)$,\\1%s\\2,\'' % Path.cwd(),
    where='local',
    force=True,
    reload=False,
)

if to_save:
    ds.save(
        path=list(to_save),
        message="Configure as a kdenlive project dataset",
    )
