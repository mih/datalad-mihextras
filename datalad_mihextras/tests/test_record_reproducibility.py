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

from os.path import join as opj
from distutils.version import LooseVersion

import datalad
from datalad.tests.utils import (
    assert_in,
    assert_in_results,
    assert_not_in,
    assert_raises,
    assert_result_count,
    eq_,
    swallow_outputs,
    with_tempfile,
    with_tree,
)

from datalad.distribution.dataset import Dataset
from datalad_mihextras import record_reproducibility
from datalad_mihextras.record_reproducibility import format_var_regex


def test_formatvar_findall():
    for in_, out in (
            ('', []),
            # invalid variable names
            (' { stupid 333}', []),
            # braced specifications
            (' {{braced}} ', []),
            ('{{braced}}', []),
            ('{{braced}} ', []),
            # regular specifications, exclude all formation and item selection
            ('{_var4_} ', ['_var4_']),
            ('{_var4_[0]} ', ['_var4_']),
            ('{_var4_.prop} ', ['_var4_']),
            ('{_var4_:format} ', ['_var4_']),
            # some more complex test
            ('{first} some {he_3re} {} this {last} {{verylast}}',
             ['first', 'he_3re', 'last']),
    ):
        eq_(format_var_regex.findall(in_), out)


@with_tempfile
def test_basic_annotation(path):
    ds = Dataset(path).create(result_renderer='disabled')
    # this is the target to reproduce
    (ds.pathobj / 'file1.txt').write_text('123\n')
    ds.save(result_renderer='disabled')
    res = ds.record_reproducibility(
        'echo "123" > file1.txt',
        outputs=['file1.txt'],
    )
    #from pprint import pprint
    #pprint(res)


@with_tempfile
def test_basic_errors(path):
    ds = Dataset(path).create(result_renderer='disabled')
    # this is the target to reproduce
    (ds.pathobj / 'file1.txt').write_text('123\n')
    ds.save(result_renderer='disabled')

    # no outputs, raise
    assert_raises(ValueError, ds.record_reproducibility)
    assert_raises(ValueError, ds.record_reproducibility, cmd='echo')

    res = ds.record_reproducibility(
        'echo', outputs=['file1.txt'],
        on_failure='ignore')
    assert_in_results(
        res,
        action='record_reproducibility',
        status='error',
        message='illegal dataset modification',
        path=str(ds.pathobj / 'file1.txt'),
    )
    from pprint import pprint
    pprint(res)


# TODO

# - check that a subdataset addition is detected and refused
