# MIH's extra DataLad functionality

[![Build status](https://ci.appveyor.com/api/projects/status/f65qpwkv2rueii1d/branch/master?svg=true)](https://ci.appveyor.com/project/mih/datalad-mihextras/branch/master) [![codecov.io](https://codecov.io/github/mih/datalad-mihextras/coverage.svg?branch=master)](https://codecov.io/github/mih/datalad-mihextras?branch=master) [![docs](https://github.com/mih/datalad-mihextras/workflows/docs/badge.svg)](https://datalad-mihextras.readthedocs.io)

DataLad extension with special interest functionality or drafts of future
additions to DataLad proper.

This is a staging area to test-drive new functionality in the wild without
making any promises on longevity or stability. Once a certain level of maturity
has been reached, most functionality will move to more appropriate places, like
topical extensions or even DataLad proper.

Command(s) provided by this extension

- `x-configuration` -- manipulate dataset, dataset-clone-local, or global
  configuration, or dump an annotated list of all effective settings
- `x-export-bagit` -- export datasets (recursively) to an RFC8493-compliant
  BagIt "bag"
- `x-export-to-webdav` -- export datasets (recursively) to any WEBDAV
  service, such as Nextcloud, box.com, or 4shared.com
- `x-snakemake` -- thin wrapper around [SnakeMake](https://snakemake.github.io)
  to obtain file content prior processing
- `git-remote-datalad-annex` -- Git remote helper to push/fetch to any
  location accessible by any git-annex special remote.
- `git-annex-backend-XDLRA` -- custom external git-annex backend used by
  `git-remote-datalad-annex`. A base class to facilitate development of
  external backends in Python is also provided.

Dataset procedure(s) provided by this extension

- `cfg_kdenlive` -- configures a dataset for use as a KDENLIVE project.
  Manages commits of the project file to be more portable, and configures
  a standard set of temporary directories for KDENLIVE.
