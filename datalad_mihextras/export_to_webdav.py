# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Export a dataset to a WEBDAV-enabled service"""

__docformat__ = 'restructuredtext'


import logging
from os import environ

from datalad.distribution.dataset import (
    EnsureDataset,
    datasetmethod,
    require_dataset,
)
from datalad.interface.base import (
    Interface,
    build_doc,
)
from datalad.interface.common_opts import (
    recursion_limit,
    recursion_flag,
)
from datalad.interface.results import (
    annexjson2result,
    get_status_dict,
)
from datalad.interface.utils import (
    eval_results,
)
from datalad.support.exceptions import (
    CommandError,
)
from datalad.support.param import Parameter
from datalad.support.constraints import (
    EnsureNone,
    EnsureStr,
)
from datalad.downloaders.credentials import UserPassword
from datalad import ui
from datalad.support.network import URL


lgr = logging.getLogger('datalad.mihextras.export_to_webdav')


@build_doc
class ExportToWEBDAV(Interface):

    _params_ = dict(
        dataset=Parameter(
            args=("-d", "--dataset"),
            doc="""specify the dataset to export""",
            constraints=EnsureDataset() | EnsureNone()),
        to=Parameter(
            args=("to",),
            metavar='NAME',
            doc="""name of the WEBDAV service""",
            constraints=EnsureStr() | EnsureNone()),
        url=Parameter(
            args=("--url",),
            metavar='URL',
            doc="""url of the WEBDAV service""",
            constraints=EnsureStr() | EnsureNone()),
        recursive=recursion_flag,
        recursion_limit=recursion_limit,
    )

    @staticmethod
    @datasetmethod(name='x_export_to_webdav')
    @eval_results
    def __call__(
            to,
            url=None,
            dataset=None,
            recursive=False,
            recursion_limit=None):

        ds = require_dataset(
            dataset,
            check_installed=True,
            purpose='exporting to WEBDAV')

        res_kwargs = dict(
            action='export_to_webdav',
            logger=lgr,
            ds=ds,
        )

        dsrepo = ds.repo

        known_special_remotes = dsrepo.get_special_remotes()
        if to is None:
            lgr.debug('No WEBDAV export target given, trying to guess')
            # no target given, guess
            webdav_cands = [
                sr['name']
                for sr in known_special_remotes.values()
                if sr.get('type') == 'webdav' and sr.get('exporttree') == 'yes'
            ]
            if len(webdav_cands) == 1:
                # a single webdav special remote with enabled export found,
                # this must be it
                to = webdav_cands[0]
                lgr.debug("Using preconfigured WEBDAV target '%s'", to)
            elif len(webdav_cands) > 1:
                raise ValueError(
                    'No WEBDAV target given, and multiple candidates are '
                    'available: {}'.format(webdav_cands))

        if to is None:
            raise ValueError(
                'No WEBDAV target given, and none could be auto-determined')

        creds = None

        matching_special_remote = [
            sr for sr in known_special_remotes.values() if sr.get('name') == to
        ]
        if matching_special_remote:
            if len(matching_special_remote) > 1:
                raise RuntimeError(
                    "Found more than one special remote with name '{}'".format(
                        to))
            msr = matching_special_remote[0]
            if not (msr.get('type') == 'webdav' and \
                    msr.get('exporttree') == 'yes'):
                raise ValueError(
                    "Special remote '{}' is not of WEBDAV type or lacks "
                    "exporttree configuration, unusable".format(to))
            # we have a properly configured `to`
            if url and not msr.get('url') == url:
                raise ValueError(
                    "A special remote '{}' already exists, but with a "
                    "different URL ({}) than the given one".format(
                        to, msr.get('url')))
            # for getting credentials
            url = msr.get('url')
        else:
            if not url:
                raise ValueError(
                    "Unknown WEBDAV special remote '{}'"
                    "and no URL provided".format(to))
            creds = get_credentials(to, url)
            _init_remote(dsrepo, to, url, creds)

        export_treeish = dsrepo.get_hexsha()

        # --json-progress gives per file progress, but no overall progress
        export_cmd = ['export', '--json-progress', export_treeish, '--to', to]

        try:
            from unittest.mock import patch
            with patch.dict('os.environ', creds or get_credentials(to, url)):
                for res in dsrepo.call_annex_records(export_cmd):
                    # https://github.com/datalad/datalad/issues/5490
                    if res.get('file', False) is None:
                        res.pop('file')
                    res = annexjson2result(res, **res_kwargs)
                    res['action'] = res_kwargs['action']
                yield res
        except CommandError as e:
            print(e)
            for res in e.kwargs.get('stdout_json', []):
                # https://github.com/datalad/datalad/issues/5490
                if res.get('file', False) is None:
                    res.pop('file')
                res['action'] = res_kwargs['action']
                yield annexjson2result(res, **res_kwargs)
            yield get_status_dict(
                status='error',
                message='export failed',
                **res_kwargs)
            return

        yield get_status_dict(
            status='ok',
            **res_kwargs)


def _init_remote(repo, name, url, creds):
    from unittest.mock import patch
    with patch.dict('os.environ', creds):
        repo.call_annex([
            'initremote',
            name,
            'type=webdav',
            'url={}'.format(url),
            'encryption=none',
            'exporttree=yes',
        ])


def get_credentials(name, url, allow_interactive=True):
    # use the hostname (only) to store these credentials under
    # this has limitations (no support for different webdav accounts on the
    # same service for different datasets
    # but otherwise we would need one set of stored credentials per each
    # dataset
    name = URL(url).hostname
    spec = dict(user='WEBDAV_USERNAME', password='WEBDAV_PASSWORD')

    # prefer the environment
    if all(k in environ for k in spec.values()):
        return {
            v: environ.get(v)
            for k, v in spec.items()
        }

    # fall back on DataLad credential manager
    up_auth = UserPassword(name=name, url=url)

    do_interactive = allow_interactive and ui.is_interactive()

    # get auth, from environment, or from datalad credential store
    # if known-- we do not support first-time entry during a test run
    return {
        v: environ.get(
            v,
            up_auth().get(k, None)
            if do_interactive or up_auth.is_known else None)
        for k, v in spec.items()
    }
