from src.main import load_game_ids_from_txt

def test_load_game_ids_only_numbers(tmp_path: str, monkeypatch: any) -> None:
    test_file = tmp_path / "id.txt"
    test_file.write_text("123\nabc\n456\n\n789")

    monkeypatch.setattr("src.main.os.path.exists", lambda _: True)
    real_open = open
    monkeypatch.setattr("builtins.open", lambda *a, **k: real_open(test_file))

    result = load_game_ids_from_txt()

    assert result == [123, 456, 789]
