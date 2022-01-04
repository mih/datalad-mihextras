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
from datalad.distribution.dataset import Dataset
from datalad.utils import chpwd

from datalad.tests.utils import (
    assert_status,
    eq_,
    nok_,
    skip_if_no_module,
    with_tempfile,
    with_tree,
)

# no point in testing without snakemake
skip_if_no_module('snakemake')

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


@with_tree(tree={'Snakefile': snakefile,
                 'test_input.txt': 'random string 123'})
@with_tempfile()
def test_snakemake_fileget(origpath, clonepath):
    ds = Dataset(origpath).create(force=True)
    assert_status('ok', ds.save(path='Snakefile', to_git=True))
    assert_status('ok', ds.save(path='test_input.txt', to_git=False))

    cln = clone(origpath, clonepath)
    # from datalad 0.16 onwards it could be
    #nok_(cln.repo.get_file_annexinfo(
    #    'test_input.txt', eval_availability=True)['has_content'])
    nok_(cln.repo.file_has_content('test_input.txt'))

    with chpwd(cln.path):
        cln.snakemake(['--', '-c1'])

    eq_((cln.pathobj / "test_output.txt").read_text(), 'random string 123')
