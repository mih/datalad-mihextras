# ex: set sts=4 ts=4 sw=4 noet:
# -*- coding: utf-8 -*-
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""

"""
from datalad.api import clone
from datalad_next.datasets import Dataset
from datalad_next.utils import chpwd

from datalad_next.tests.utils import (
    SkipTest,
    assert_status,
)

# no point in testing without snakemake
try:
    import snakemake
except ImportError as e:
    raise SkipTest('No snakemake installed') from e


# ensure we have the snakemake dataset method
from datalad_mihextras.snakemake import SnakeMake


# workflow that copies an input file into an output file
snakefile = """\
rule test:
    input:
        "test_input.txt"
    output:
        "test_output.txt"
    run:
        with open(output[0], "w") as out, open(input[0]) as inp:
            out.write(inp.read())
"""


def test_snakemake_fileget(tmp_path):
    origpath = tmp_path / 'orig'
    origpath.mkdir()
    (origpath / 'Snakefile').write_text(snakefile)
    (origpath / 'test_input.txt').write_text('random string 123')
    ds = Dataset(origpath).create(force=True)
    assert_status('ok', ds.save(path='Snakefile', to_git=True))
    assert_status('ok', ds.save(path='test_input.txt', to_git=False))

    cln = clone(origpath, tmp_path / 'clone')
    # from datalad 0.16 onwards it could be
    assert not cln.repo.get_file_annexinfo(
        'test_input.txt',
        eval_availability=True)['has_content']

    with chpwd(cln.path):
        cln.snakemake(['--', '-c1'])

    assert (cln.pathobj / "test_output.txt").read_text() == 'random string 123'
