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
            'datalad_mihextras.configuration',
            'Configuration',
            'x-configuration',
            'x_configuration'
        ),
        (
            'datalad_mihextras.snakemake',
            'SnakeMake',
            'x-snakemake',
            'x_snakemake'
        ),
    ]
)


from datalad import setup_package
from datalad import teardown_package

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
