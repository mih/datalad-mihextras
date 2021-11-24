# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Export a dataset to a Bag-it

https://www.rfc-editor.org/rfc/rfc8493.html
"""

__docformat__ = 'restructuredtext'


import logging
from itertools import chain
from pathlib import (
    Path,
)
from shutil import copyfile

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
    get_status_dict,
)
from datalad.interface.utils import (
    eval_results,
)
from datalad.support.param import Parameter
from datalad.support.constraints import (
    EnsureChoice,
    EnsureNone,
    EnsureStr,
)


lgr = logging.getLogger('datalad.mihextras.export_bagit')


@build_doc
class ExportBagit(Interface):
    """Export a dataset to a Bag-it

    This is a proof-of-principle implementation that can export a DataLad
    dataset into a BagIt bag, a standardized storage and and transfer
    format for arbitrary digital content.

    TODOs:

    - Support bag-of-bags
      https://github.com/fair-research/bdbag/tree/master/examples/bagofbags
    - Support for hardlinking local files into the bag so save space and time
      on export
    - Support for automatically missing content on export
    - Support for bag metadata specification

    .. seealso::

       `RFC8493 <https://www.rfc-editor.org/rfc/rfc8493.html>`_
          BagIt specification.
    """
    _examples_ = [
        dict(text="Export dataset to a bag directory at /tmp/bag",
             code_py="x_export_bagit('/tmp/bag')",
             code_cmd="datalad x-export-bagit /tmp/bag"),
        dict(text="Export dataset to a ZIP archive bag at /tmp/bag.zip",
             code_py="x_export_bagit('/tmp/bag', archive='zip')",
             code_cmd="datalad x-export-bagit --archive zip /tmp/bag"),
    ]

    _params_ = dict(
        dataset=Parameter(
            args=("-d", "--dataset"),
            doc="""specify the dataset to export""",
            constraints=EnsureDataset() | EnsureNone()),
        to=Parameter(
            args=("to",),
            metavar='PATH',
            doc="""location to export to.
            With [CMD: --archive CMD][PY: `archive` PY] this is the base path,
            and a filename extension will be appended to it.""",
            constraints=EnsureStr() | EnsureNone()),
        archive=Parameter(
            args=("--archive", ),
            doc="""export bag as a single-file archive in the given format""",
            constraints=EnsureChoice('tar', 'tgz', 'bz2', 'zip', None)),
        recursive=recursion_flag,
        recursion_limit=recursion_limit,
    )

    @staticmethod
    @datasetmethod(name='x_export_bagit')
    @eval_results
    def __call__(
            to,
            archive=None,
            dataset=None,
            recursive=False,
            recursion_limit=None):

        ds = require_dataset(
            dataset,
            check_installed=True,
            purpose='exporting to BagIt')

        res_kwargs = dict(
            action='export_bagit',
            logger=lgr,
        )

        datasets = [[ds]]
        if recursive:
            datasets.append(
                ds.subdatasets(
                    fulfilled=True,
                    recursive=recursive,
                    recursion_limit=recursion_limit,
                    return_type='generator',
                    result_renderer='disabled',
                    result_xfm='datasets',
                )
            )

        to = Path(to)
        if not to.exists():
            to.mkdir(exist_ok=True, parents=True)

        # TODO this reconfigures DataLad log handling and doubles all reporting
        from bdbag import bdbag_api as bi
        bag = bi.make_bag(str(to))

        for d in chain(*datasets):
            try:
                for res in _export_bagit(
                        ds,
                        d,
                        bag):
                    yield dict(
                        get_status_dict(ds=d, **res_kwargs),
                        **res)
            except ValueError as e:
                yield get_status_dict(
                    ds=d,
                    status='error',
                    message=str(e),
                    **res_kwargs)
        bag.save(manifests=True)
        bag.validate(completeness_only=True)
        if archive:
            archive_path = bi.archive_bag(bag.path, archive)
            yield get_status_dict(
                status='ok',
                type='bag',
                path=archive_path,
                **res_kwargs)


def _get_key_urls(repo, rstatus):
    # format all found annex keys as suitable input for a batched `whereis`
    all_annex_keys = b'\n'.join(
        bytes(r['key'], 'utf-8') for r in rstatus if 'key' in r)

    lgr.info('Get whereis')
    # and now, only for annex repos, ask for URLs
    arecs = repo._call_annex([
        'whereis',
        # doesn't hurt
        '--fast',
        '--batch-keys',
        "--format=${key}\t${url}\n",
        # we know the worktree is clean,
        # but maybe we can avoid a few filesystem ops
        '--branch', 'HEAD'],
        stdin=all_annex_keys)

    lgr.info('Map keys to URLs')
    key_urls = {}
    for r in arecs['stdout'].splitlines():
        k, u = r.split('\t', maxsplit=1)
        urls = key_urls.get(k, [])
        urls.append(u)
        key_urls[k] = urls
    return key_urls


def _export_bagit(rootds, ds, bag):
    """ """
    repo = ds.repo
    export_treeish = repo.get_hexsha()
    if export_treeish is None:
        # this is impossible to reach for a registered subdataset,
        # hence should be fine as an exception (not an error-result)
        raise ValueError('No saved dataset state found')

    return_props = dict(
    )

    has_annex = hasattr(repo, 'call_annex')
    bag_path = Path(bag.path)

    lgr.info('Get status')

    # the status call serves two purposes simultaneously
    # 1. make sure the dataset is clean -- we only want to export a known state
    # 2. distinguish git from annex'ed content -- important further down
    rstatus = ds.status(
        # we need to inspect the keys further down, but only for annex repos
        annex='basic' if has_annex else None,
        # anything unracked is intollerable, we can fail on the cheapest report
        untracked='normal',
        # each subdataset is processed individually
        eval_subdataset_state='no',
        result_renderer='disabled',
    )

    # get the mapping of annex keys to URLs, if needed
    key_urls = _get_key_urls(repo, rstatus) if has_annex else {}

    for rec in rstatus:
        key = rec.get('key')
        backend = rec.get('backend', '').lower()
        digest = rec.get('keyname')
        filepath = Path(rec['path'])
        # TODO support switch to disable any remote files
        if not key or backend == 'url' or key not in key_urls:
            # this is not an annexed file, or one with an key that doesn't have
            # digest and size info, or a key without an associated URL
            # copy into the bag
            target_path = \
                bag_path / 'data' / filepath.relative_to(rootds.pathobj)
            target_path.parent.mkdir(exist_ok=True, parents=True)
            # TODO ability to hardlink, if possible
            copyfile(filepath, target_path, follow_symlinks=True)
            message = 'copied into bag'
        else:
            # we can register it as a remote file
            if backend.endswith('e'):
                # adjust for presence of file name extension
                backend = backend[:-1]
                digest = digest.split('.', maxsplit=1)[0]
            # bagit always used relative path in POSIX convention
            posix_relpath = filepath.relative_to(rootds.pathobj).as_posix()
            bag.add_remote_file(
                posix_relpath,
                key_urls[key][0],
                rec['bytesize'],
                backend,
                digest,
            )
            message = 'registered as a remote file'
        yield get_status_dict(
            status='ok',
            path=str(filepath),
            type='file',
            message=message,
            **return_props)

    yield get_status_dict(
        status='ok',
        **return_props)
