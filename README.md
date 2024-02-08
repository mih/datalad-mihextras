# MIH's extra DataLad functionality

[![Build status](https://ci.appveyor.com/api/projects/status/f65qpwkv2rueii1d/branch/main?svg=true)](https://ci.appveyor.com/project/mih/datalad-mihextras/branch/main)
[![codecov](https://codecov.io/github/mih/datalad-mihextras/graph/badge.svg?token=bfZTqJAYRV)](https://codecov.io/github/mih/datalad-mihextras)
[![docs](https://github.com/mih/datalad-mihextras/workflows/docs/badge.svg)](https://datalad-mihextras.readthedocs.io)

DataLad extension with special interest functionality or drafts of future
additions to DataLad proper.

This is a staging area to test-drive new functionality in the wild without
making any promises on longevity or stability. Once a certain level of maturity
has been reached, most functionality will move to more appropriate places, like
topical extensions or even DataLad proper.

Command(s) provided by this extension

- `x-export-bagit` -- export datasets (recursively) to an RFC8493-compliant
  BagIt "bag"
- `x-snakemake` -- thin wrapper around [SnakeMake](https://snakemake.github.io)
  to obtain file content prior processing

Dataset procedure(s) provided by this extension

- `cfg_kdenlive` -- configures a dataset for use as a KDENLIVE project.
  Manages commits of the project file to be more portable, and configures
  a standard set of temporary directories for KDENLIVE.

Once available functionality

- `x-configuration` has been migrated to the core DataLad package under the name
  `configuration`.
- `git-remote-datalad-annex` -- has been migrated to an improved implementation
  provided by https://github.com/datalad/datalad-next
- `git-annex-backend-XDLRA`, and the base class to facilitate development of
  external backends in Python -- has been migrated to an improved implementation
  provided by https://github.com/datalad/datalad-next
- `x-export-to-webdav` -- is discontinued. https://github.com/datalad/datalad-next
  provides `create-sibling-webdav` (supports export remotes) and enhances the
  standard `push` command to be able to handle exports automatically. This
  removed the need for a custom implementation.
