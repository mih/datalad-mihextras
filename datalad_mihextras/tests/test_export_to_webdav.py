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
    assert_not_in_results,
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
from datalad.support.external_versions import external_versions as exv
annex_version = exv['cmd:annex']


_dataset_config_template = {
    'ingit.txt': 'ingit',
    'subdir': {
        'inannex.txt': 'inannex'
    },
    'sub dir2': {
        'inannex2.txt': 'inannex2'
    },
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
        webdav.clean(dsid)
    except webdav_exc.RemoteResourceNotFound:
        # that is what we wanted to achieve
        print("HELP! CANNOT CLEAN UP!")


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
        _test_retrieval(webdav, ds)
        if annex_version > '8.20210311':
            # older versions did not exclude subdatasets
            _test_recursive_export(webdav, ds)
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
    if annex_version > '8.20210311':
        # older versions did not set the file property
        assert_in_results(
            res,
            path=str(annexfile_pathobj),
            status='ok',
            type='file',
        )
    # delete something remote without git-annex knowing
    webdav.clean('{}/subdir'.format(ds.id))
    # default mode of operation will miss this, and will not
    # reupload
    ds.x_export_to_webdav(to='webdav')
    assert_not_in('subdir', webdav.list(ds.id))
    # but we can make it test for that
    ds.x_export_to_webdav(to='webdav', mode='verify')
    assert_in('inannex.txt', webdav.list('{}/subdir'.format(ds.id)))


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


def _test_recursive_export(webdav, ds):
    # create a subdataset
    subds = ds.create(ds.pathobj / 'sub dir2' / 'subds')
    # re-export
    ds.x_export_to_webdav(to='webdav')
    # must not contain a block for a later nested subds export
    assert_not_in('subds', webdav.list('{}/sub dir2'.format(ds.id)))

    res = ds.x_export_to_webdav(to='webdav', recursive=True)
    # subdataset was published
    assert_in_results(res, path=subds.path, status='ok', type='dataset')
    # with content
    assert_in_results(res, path=str(subds.pathobj / '.datalad' / 'config'),
                      status='ok', type='file')
    # confirm on webdav
    assert_in('config', webdav.list('{}/{}'.format(
        ds.id,
        str((subds.pathobj / '.datalad').relative_to(ds.pathobj))
    )))
