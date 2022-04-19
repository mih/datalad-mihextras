# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 et:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Record if arbitrary commands can reproduce dataset content"""

__docformat__ = 'restructuredtext'


import logging
from pathlib import Path
import re
from urllib.parse import urlencode

from datalad.distribution.dataset import (
    datasetmethod,
    require_dataset,
)
from datalad.interface.base import (
    Interface,
    build_doc,
)
from datalad.interface.utils import (
    eval_results,
)
from datalad.core.local.run import (
    Run,
    run_command,
)
from datalad.core.local.status import get_paths_by_ds
from datalad.interface.results import get_status_dict
from datalad.support.annexrepo import AnnexRepo

lgr = logging.getLogger('datalad.mihextras.record_reproducibility')

# regex to discover all named substitution from a string
format_var_regex = re.compile(r'(?:[^{]|^){([A-Za-z_][A-Za-z0-9_]*)[^}]*}')


@build_doc
class RecordReproducibility(Interface):
    """
    """
    # make run stop immediately on non-success results.
    # this prevents command execution after failure to obtain inputs of prepare
    # outputs. but it can be overriding via the common 'on_failure' parameter
    # if needed.
    on_failure = 'stop'

    _params_ = {
        c: Run._params_[c]
        for c in ('cmd', 'dataset', 'inputs', 'outputs', 'expand',
                  'dry_run', 'jobs')
    }

    @staticmethod
    @datasetmethod(name='record_reproducibility')
    @eval_results
    def __call__(
            cmd=None,
            *,
            dataset=None,
            inputs=None,
            outputs=None,
            expand=None,
            dry_run=None,
            jobs=None):
        # TODO move everything in here into a function that is a
        # drop-in replacement for `run_command()` such that a command
        # like `containers_run()` or a similar command in the containers
        # extension can use it easily
        # TODO it makes no sense to call this command without any outputs
        # specified
        if not outputs:
            raise ValueError(
                'recording a reproducibility record requires a declaration '
                'of outputs to identify the files that are to be reproduced.')
        ds = require_dataset(
            dataset,
            check_installed=True,
            purpose='annotate dataset with reproducibility record')
        repo = ds.repo
        if repo.dirty:
            yield get_status_dict(
                'record_reproducibility',
                ds=ds,
                status='impossible',
                message=(
                    'clean dataset required to verify reproducibility by '
                    'command; use `datalad status` to inspect unsaved '
                    'changes'))
            return

        # this is where we start
        pre_state = _get_repo_refstate(repo)

        # critical state info
        run_info = None
        record_id = None
        reported_outputs = None
        # and ... launch
        for r in run_command(
                cmd,
                # reuse pristine argument to retain argument semantics
                dataset=dataset,
                inputs=inputs,
                outputs=outputs,
                expand=expand,
                assume_ready=None,
                explicit=False,
                # TODO this message we either be attached to a commit with
                # illegal modifications of a dataset, or with the addition
                # of a parametric run record sidecar file
                message="DUMMY",
                sidecar=True,
                dry_run=dry_run,
                jobs=jobs,
                # we want a single record to work recomputing many outputs
                # individually
                parametric_record=True,
                # we must make sure that any declared outputs are (re)produced
                # because we want to annotate these specifically with
                # reproducibility records. Removing them prior execution
                # is the cheapest test for that
                remove_outputs=True,
                # we tested for dirty above already
                skip_dirtycheck=True,
                # need to annotate the outputs, but maybe only those that were
                # no also inputs
                yield_expanded='both'):
            # TODO do we actually want to communicate the internal, in case
            # there is no error?
            # but we need to yield in order to get the flow control
            # from on_failure=
            yield r
            if r.get('action') == 'run' and r.get('record_id'):
                # this is the jackpot: run result with with run record id
                run_info = r['run_info']
                record_id = r['record_id']
                reported_outputs = r['expanded_outputs']

        if not reported_outputs:
            yield get_status_dict(
                action='record_reproducibility',
                status='impossible',
                message='no outputs to annotate (paths do not exist)',
                ds=ds,
                refds=ds.path,
            )
            # no point in any further checks, nothing here to be annotated
            return

        # we need this for all repro-records
        assert(record_id)

        # this is where we ended up
        post_state = _get_repo_refstate(repo)
        if pre_state != post_state:
            # we must take a closer look, only the addition of a run record
            # sidecar is allowed
            illegal_mods = False
            for r in _yield_illegal_modifications(ds, pre_state, post_state):
                if r.get('status') != 'ok':
                    # TODO only here we go issue a hint that the dataset
                    # must be reset, before one can try again
                    illegal_mods = True
                yield r
            if illegal_mods:
                # all error results made it out already, we just need to
                # stop processing, there cannot be reproducibility
                return

        # build the URL (it will be the same for all annex keys
        repro_url = _build_repro_url(
            run_info['dsid'],
            post_state,
            record_id,
            # determine the parameters required to complete the repro record
            _get_substitutions_to_save(ds, run_info),
        )

        # use get_paths_by_ds() to be able to call AnnexRepo.get_content_annexinfo()
        outpaths_by_ds, errpaths = get_paths_by_ds(ds, ds, reported_outputs)
        # we cannot have errors here, because we know all paths exist
        # but alert on surprises
        assert(not errpaths)

        addurls_spec = []
        while outpaths_by_ds:
            dpath, outpaths = outpaths_by_ds.popitem()
            info = AnnexRepo(dpath).get_content_annexinfo(outpaths, init=None)
            while info:
                fpath, props = info.popitem()
                key = props.get('key')
                if not key:
                    continue
                addurls_spec.append(dict(
                    url=repro_url,
                    path=str(fpath),
                    key=key,
                ))

        # at this point the dataset in in good shape and `post_state`
        # is the commit we want to record in the annotations.
        # moreover, the outputs must all be there

        yield from ds.addurls(
            addurls_spec,
            '{url}',
            '{path}',
            # passing a key will prevent git-annex from making any actual
            # web requests
            key='{key}',
            # there is nothing in the URL that we would want to become metadata
            exclude_autometa='*',
            # no "download" desired, all content already exists locally
            fast=True,
            # this command would not modify the active branch
            save=False,
            # we only want annotation, no other state change
            drop_after=False,
            # seems to make no sense to reuse the jobs specification here,
            # because no downloads are performed
            #jobs=jobs,
            # yield immediately
            return_type='generator',
            # rendering choice belongs to the parent
            result_renderer='disabled',
            # on_failure mode is parent's choice
            on_failure='ignore',
        )
        # TODO verify that no further commit was made

    @staticmethod
    def custom_result_renderer(res, **kwargs):
        return Run.custom_result_renderer(res, **kwargs)


def _build_repro_url(dsid, refcommit, recordid, params):
    # doseq=True to be able to handle sequence-type parameter values
    query = urlencode(params, doseq=True)
    return f'datalad-repro://{refcommit}@{dsid}/{recordid}?{query}'


def _get_substitutions_to_save(ds, runinfo):
    allvars = set()
    for v in (
            [runinfo['cmd']],
            runinfo['inputs'],
            runinfo['extra_inputs'],
            runinfo['outputs']):
        for i in v:
            allvars.update(format_var_regex.findall(i))
    # ignore all variable that are dynamically provided by `run`
    allvars.difference_update(('pwd', 'dspath', 'tmpdir', 'inputs', 'outputs'))

    # localize import of private helper to avoid immediate crash-on-import
    # if core package changes
    from datalad.core.local.run import _get_substitutions
    subst = _get_substitutions(ds)

    return {k: subst[k] for k in allvars}


def _get_repo_refstate(repo):
    # make sure we have an up-to-date corresponding branch
    # (if there is any)
    repo.localsync(managed_only=True)
    ref_branch = repo.get_corresponding_branch()
    return repo.get_hexsha(ref_branch)


def _yield_illegal_modifications(ds, pre, post):
    runinfo_path = ds.pathobj / '.datalad' / 'runinfo'
    added_runrecords = 0
    for r in ds.diff(
            fr=pre, to=post,
            # it is fine to no go recursive, any added subdataset
            # is already a pr
            recursive=False,
            annex=None,
            return_type='generator',
            result_renderer='disabled'):
        if r.get('action') != 'diff':
            # ignore possible non-diff results
            continue
        if r['state'] == 'clean':
            # this is safe
            continue
        if r['state'] == 'added' and \
                not added_runrecords and \
                runinfo_path in Path(r['path']).parents:
            # this is an added run-record side care file, fine too
            added_runrecords += 1
            continue
        # everything else is a problem
        r.update(
            action='record_reproducibility',
            status='error',
            message='illegal dataset modification',
        )
        yield r
