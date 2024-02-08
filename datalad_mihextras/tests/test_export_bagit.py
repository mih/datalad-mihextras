from datalad.api import x_export_bagit

from datalad_next.runners import call_git_success


def test_export_bagit(no_result_rendering, existing_dataset, tmp_path):
    ds = existing_dataset
    fileurl = 'https://upload.wikimedia.org/wikipedia/commons/thumb/a/a8/LOC_Main_Reading_Room_Highsmith.jpg/320px-LOC_Main_Reading_Room_Highsmith.jpg'
    call_git_success([
        'annex',
        'addurl',
        fileurl,
        '--file', 'loc.jpg'],
        cwd=ds.pathobj,
        # would need datalad-next >1.2
        #capture_output=True,
    )
    ds.save()
    ds.x_export_bagit(tmp_path)
    assert (tmp_path / 'data' / '.datalad').exists()
    assert (tmp_path / 'bagit.txt').exists()
    assert 'datalad/config' in (tmp_path / 'manifest-md5.txt').read_text()
    assert fileurl in (tmp_path / 'fetch.txt').read_text()
