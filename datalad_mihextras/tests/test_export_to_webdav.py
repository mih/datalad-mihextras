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

from os import environ
from webdav3.client import Client as DAVClient
from webdav3 import exceptions as webdav_exc
from pathlib import Path

from datalad.tests.utils import (
    assert_in,
    assert_in_results,
    assert_not_in,
    assert_raises,
    assert_result_count,
    assert_status,
    eq_,
    swallow_outputs,
    with_tempfile,
    with_tree,
)

from datalad.distribution.dataset import Dataset
from datalad_mihextras import export_to_webdav


_dataset_config_template = {
    'ingit.txt': 'ingit',
    'subdir': {
        'inannex.txt': 'inannex'
    }
}

webdav_cfg = dict(
    webdav_hostname='https://dav.box.com',
    webdav_login=environ['DATALAD_dav_box_com_user'],
    webdav_password=environ['DATALAD_dav_box_com_password'],
    webdav_root='dav/datalad-tester/',
)

webdav_url_tmpl = '%s/%s/{id}' % (
    webdav_cfg['webdav_hostname'],
    webdav_cfg['webdav_root'],
)


def cleanup_webdav(webdav, dsid):
    try:
        webdav.clean('datalad-tester/{}'.format(dsid))
    except webdav_exc.RemoteResourceNotFound:
        # that is what we wanted to achieve
        pass


@with_tree(tree=_dataset_config_template)
@with_tempfile(mkdir=True)
def test_something(src, dst):
    ds = Dataset(src).create(force=True)
    ds.save(path='ingit.txt', to_git=True)
    ds.save()

    # direct client access to clean things up and inspect results
    webdav = DAVClient(webdav_cfg)

    webdav_url = webdav_url_tmpl.format(id=ds.id)
    try:
        _test_basic_export(webdav, ds, webdav_url)
        _test_repeated_export(webdav, ds)
        _test_subds_exclusion(webdav, ds)
        _test_retrieval(webdav, ds)
    finally:
        cleanup_webdav(webdav, ds.id)


def _test_basic_export(webdav, ds, webdav_url):
    assert_status(
        'ok',
        ds.x_export_to_webdav(to='webdav', url=webdav_url),
    )
    eq_(webdav.list('{}/subdir'.format(ds.id)), ['inannex.txt'])
    assert_in('ingit.txt', webdav.list(ds.id))


def _test_repeated_export(webdav, ds):
    # only the name is sufficient
    assert_status('ok', ds.x_export_to_webdav(to='webdav'))
    # now make a change to the annexed file
    ds.unlock(Path('subdir', 'inannex.txt'))
    annexfile_pathobj = ds.pathobj / 'subdir' / 'inannex.txt'
    annexfile_pathobj.write_text('update')
    ds.save()
    # the updated file is exported
    res = ds.x_export_to_webdav(to='webdav')
    assert_in_results(
        res,
        path=str(annexfile_pathobj),
        status='ok',
        type='file',
    )


def _test_subds_exclusion(webdav, ds):
    ds.create('subds')
    ds.x_export_to_webdav(to='webdav')
    assert_not_in('subds', webdav.list(ds.id))


def _test_retrieval(webdav, ds):
    annexfile_pathobj = ds.pathobj / 'subdir' / 'inannex.txt'
    ds.drop(annexfile_pathobj)
    # ensure dropped
    assert_in_results(
        ds.status(annexfile_pathobj, annex='availability',
                  result_renderer='disabled'),
        path=str(annexfile_pathobj),
        has_content=False,
    )
    ds.get(annexfile_pathobj)
    # ensure present again
    assert_in_results(
        ds.status(annexfile_pathobj, annex='availability',
                  result_renderer='disabled'),
        path=str(annexfile_pathobj),
        has_content=True,
    )
