"""MIH's DataLad extension"""

__docformat__ = 'restructuredtext'

# defines a datalad command suite
# this symbold must be indentified as a setuptools entrypoint
# to be found by datalad
command_suite = (
    # description of the command suite, displayed in cmdline help
    "MIH's DataLad extras",
    [
        # specification of a command, any number of commands can be defined
        (
            'datalad_mihextras.snakemake',
            'SnakeMake',
            'x-snakemake',
            'x_snakemake'
        ),
        (
            'datalad_mihextras.export_bagit',
            'ExportBagit',
            'x-export-bagit',
            'x_export_bagit'
        ),
    ]
)


from . import _version
__version__ = _version.get_versions()['version']
