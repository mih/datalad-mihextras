
def test_register():
    import datalad.api as da
    assert hasattr(da, 'x_export_bagit')
