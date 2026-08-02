from app.db import normalize_file_formats


def test_normalize_list_and_string():
    assert normalize_file_formats("mp4, JPG") == ["mp4", "jpg"]
    assert normalize_file_formats([".Mp4", "jpg", "jpg"]) == ["mp4", "jpg"]


def test_normalize_dict():
    out = normalize_file_formats({"Video": ["mp4", "MP4"], "photo": "jpg,png"})
    assert out == {"video": ["mp4"], "photo": ["jpg", "png"]}


def test_normalize_empty():
    assert normalize_file_formats(None) == []
    assert normalize_file_formats({}) == []
    assert normalize_file_formats("") == []
