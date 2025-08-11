from __future__ import annotations

import json


def test_artifact_fixture_writes_manifest(artifacts):
    artifacts.save_text("example.txt", "hi")
    manifest = artifacts.finalize()
    assert manifest["example.txt"] == 2
    manifest_path = artifacts.path / "manifest.json"
    data = json.loads(manifest_path.read_text())
    assert data["example.txt"] == 2
