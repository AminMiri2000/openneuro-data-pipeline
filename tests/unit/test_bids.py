from openneuro_pipeline.load.bids import parse_participants_tsv, parse_sidecar


def test_tsv_nulls_ids_and_whitespace():
    data = b"\xef\xbb\xbfparticipant_id\t age \tsex\nsub-01\t24\t F \nsub-02\tn/a\t\n"
    rows = list(parse_participants_tsv(data))

    assert [r.participant_id for r in rows] == ["sub-01", "sub-02"]
    assert rows[0].attributes == {"age": "24", "sex": "F"}  # BOM and spaces stripped
    assert rows[1].attributes == {"age": None, "sex": None}  # 'n/a' and '' -> None
    assert [r.row_number for r in rows] == [1, 2]


def test_tsv_ignores_surplus_cells():
    rows = list(parse_participants_tsv(b"participant_id\tage\nsub-01\t24\textra\n"))
    assert rows[0].attributes == {"age": "24"}


def test_sidecar_pairs_and_bad_json():
    assert dict(parse_sidecar(b'{"age": {"Units": "years"}}')) == {"age": {"Units": "years"}}
    assert list(parse_sidecar(b"{not json")) == []
    assert list(parse_sidecar(b"[1, 2]")) == []  # valid JSON, wrong shape
