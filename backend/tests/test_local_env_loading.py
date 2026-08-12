from pathlib import Path


def test_load_environment_reads_repo_root_env_when_backend_env_is_missing(tmp_path, monkeypatch):
    import server

    backend_dir = tmp_path / "backend"
    backend_dir.mkdir()
    root_env = tmp_path / ".env"
    root_env.write_text("LOCAL_ONLY_SETTING=from-root\n", encoding="utf-8")

    monkeypatch.delenv("LOCAL_ONLY_SETTING", raising=False)

    server.load_environment(backend_dir)

    assert server.os.environ["LOCAL_ONLY_SETTING"] == "from-root"


def test_load_environment_prefers_existing_process_environment(tmp_path, monkeypatch):
    import server

    backend_dir = tmp_path / "backend"
    backend_dir.mkdir()
    (tmp_path / ".env").write_text("LOCAL_ONLY_SETTING=from-root\n", encoding="utf-8")

    monkeypatch.setenv("LOCAL_ONLY_SETTING", "from-shell")

    server.load_environment(backend_dir)

    assert server.os.environ["LOCAL_ONLY_SETTING"] == "from-shell"
