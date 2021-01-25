from datalad.tests.utils import assert_status


def test_register():
    import datalad.api as da
    assert hasattr(da, 'x_configuration')
    assert_status(
        'ok',
        da.x_configuration(result_renderer='disabled'),)
