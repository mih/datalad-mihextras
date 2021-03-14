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

from datalad.tests.utils import (
    assert_in,
    assert_in_results,
    assert_not_in,
    assert_raises,
    assert_result_count,
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
    webdav_root='dav/',
)

webdav_url_tmpl = '%s/%s/datalad-tester/{id}' % (
    webdav_cfg['webdav_hostname'],
    webdav_cfg['webdav_root'],
)


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
        ds.x_export_to_webdav(to='webdav', url=webdav_url)
    finally:
        webdav.clean('datalad-tester/{}'.format(ds.id))
