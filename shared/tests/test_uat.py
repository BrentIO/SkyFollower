import time

from shared.uat import parse_978_line


class TestParse978Line:
    """Tests for parse_978_line — the dump978-fa line-protocol parser shared
    by the receiver and the traffic-recorder tool."""

    _DOWNLINK = "-00a3d3e328a71f8c647004e9009c2d401a00;rs=6;rssi=0.3;t=1782561034.334;"
    _UPLINK = "+08a3d3e328a75f8c653204e900742e4028066a0025ed2d0b1aa4c0a0000530000000;rssi=1.4;t=1782561038.701;"

    def test_downlink_frame_parsed(self):
        result = parse_978_line(self._DOWNLINK)
        assert result is not None
        raw_hex, icao_hex, received_at = result
        assert raw_hex == "-00A3D3E328A71F8C647004E9009C2D401A00"
        assert icao_hex == "A3D3E3"
        assert received_at == 1782561034.334

    def test_uplink_frame_parsed(self):
        result = parse_978_line(self._UPLINK)
        assert result is not None
        raw_hex, icao_hex, received_at = result
        assert raw_hex.startswith("+")
        assert received_at == 1782561038.701

    def test_icao_extracted_from_bytes_1_to_3(self):
        # "00A9E98F28C9F18C63380589113026C02800" -> byte 0 = "00" (HDR type/qualifier),
        # bytes 1-3 = "A9E98F" (the 24-bit ICAO/participant address).
        result = parse_978_line("-00A9E98F28C9F18C63380589113026C02800;t=1.0;")
        assert result is not None
        _, icao_hex, _ = result
        assert icao_hex == "A9E98F"

    def test_preamble_line_skipped(self):
        line = "!fecfix=1;program=dump978-fa;version=11.0~bpo12+1;"
        assert parse_978_line(line) is None

    def test_blank_line_skipped(self):
        assert parse_978_line("") is None
        assert parse_978_line("   ") is None

    def test_invalid_prefix_skipped(self):
        assert parse_978_line("*00a3d3e328a71f8c647004e9009c2d401a00;") is None

    def test_short_hex_payload_skipped(self):
        assert parse_978_line("-1234;t=1782561034.334;") is None

    def test_non_hex_payload_skipped(self):
        assert parse_978_line("-00zzzzzz28a71f8c647004e9009c2d401a00;t=1.0;") is None

    def test_raw_hex_uppercased(self):
        result = parse_978_line("-00a3d3e328a71f8c647004e9009c2d401a00;t=1.0;")
        assert result is not None
        raw_hex, _, _ = result
        assert raw_hex == raw_hex.upper()

    def test_timestamp_defaults_to_wallclock_when_t_missing(self):
        before = time.time()
        result = parse_978_line("-00a3d3e328a71f8c647004e9009c2d401a00;rs=6;")
        after = time.time()
        assert result is not None
        _, _, received_at = result
        assert before <= received_at <= after

    def test_malformed_t_field_falls_back_to_wallclock(self):
        before = time.time()
        result = parse_978_line("-00a3d3e328a71f8c647004e9009c2d401a00;t=notanumber;")
        after = time.time()
        assert result is not None
        _, _, received_at = result
        assert before <= received_at <= after
