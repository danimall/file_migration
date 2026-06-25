"""Tests for Dropbox entry conversion and identity hashing."""

from __future__ import annotations

from filemig.clients.dropbox_client import to_discovered
from filemig.util.identity import KEY_SRC_PATH, app_properties, src_path_sha256


def test_to_discovered_regular_file():
    entry = {
        ".tag": "file", "id": "id:1", "path_lower": "/photos/a.jpg",
        "path_display": "/Photos/A.jpg", "rev": "0123", "size": 42,
        "content_hash": "c" * 64, "is_downloadable": True,
    }
    df = to_discovered(entry)
    assert df is not None
    assert df.parent_path_lower == "/photos"
    assert df.is_downloadable
    assert df.export_format is None
    assert df.content_hash == "c" * 64


def test_to_discovered_cloud_doc_uses_export_format():
    entry = {
        ".tag": "file", "id": "id:2", "path_lower": "/docs/plan.gdoc",
        "path_display": "/Docs/plan.gdoc", "rev": "abcd", "size": 0,
        "is_downloadable": False,
        "export_info": {"export_as": "docx"},
    }
    df = to_discovered(entry)
    assert df is not None
    assert not df.is_downloadable
    assert df.export_format == "docx"
    assert df.content_hash is None


def test_to_discovered_skips_folders():
    assert to_discovered({".tag": "folder", "path_lower": "/x"}) is None


def test_to_discovered_marks_shared():
    entry = {
        ".tag": "file", "id": "id:3", "path_lower": "/shared/x", "path_display": "/Shared/x",
        "rev": "r", "size": 1, "content_hash": "a" * 64, "is_downloadable": True,
        "sharing_info": {"parent_shared_folder_id": "123"},
    }
    assert to_discovered(entry).is_shared


def test_src_path_sha256_stable_and_path_specific():
    a = src_path_sha256("run-1", "/photos/a.jpg")
    assert a == src_path_sha256("run-1", "/photos/a.jpg")
    assert a != src_path_sha256("run-1", "/photos/b.jpg")
    assert a != src_path_sha256("run-2", "/photos/a.jpg")


def test_app_properties_respect_124_byte_limit():
    props = app_properties("run-uuid-1234", "f" * 64, "c" * 64)
    assert props[KEY_SRC_PATH] == "f" * 64
    for k, v in props.items():
        assert len(k) + len(v) <= 124
