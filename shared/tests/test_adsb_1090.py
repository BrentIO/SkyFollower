from shared.adsb_1090 import parse_tcp_stream


class TestParseTcpStream:
    """Tests for parse_tcp_stream — the readsb raw-format (*hex;) parser
    shared by the receiver and the traffic-recorder tool."""

    def test_single_complete_message(self):
        data = b"*8D4B1900EA11DA58A9123456;"
        buf = bytearray()
        msgs = parse_tcp_stream(data, buf)
        assert msgs == ["8D4B1900EA11DA58A9123456"]

    def test_multiple_messages_in_one_chunk(self):
        data = b"*AABBCC;*DDEEFF;"
        buf = bytearray()
        msgs = parse_tcp_stream(data, buf)
        assert msgs == ["AABBCC", "DDEEFF"]

    def test_lowercase_hex_normalised_to_upper(self):
        data = b"*aabbcc;"
        buf = bytearray()
        msgs = parse_tcp_stream(data, buf)
        assert msgs == ["AABBCC"]

    def test_message_split_across_chunks(self):
        buf = bytearray()
        msgs1 = parse_tcp_stream(b"*AABB", buf)
        assert msgs1 == []
        msgs2 = parse_tcp_stream(b"CC;", buf)
        assert msgs2 == ["AABBCC"]

    def test_newline_between_messages_ignored(self):
        data = b"*AABBCC;\n*DDEEFF;\n"
        buf = bytearray()
        msgs = parse_tcp_stream(data, buf)
        assert msgs == ["AABBCC", "DDEEFF"]

    def test_star_resets_partial_buffer(self):
        buf = bytearray()
        parse_tcp_stream(b"*AAAA", buf)
        msgs = parse_tcp_stream(b"*BBBBCC;", buf)
        assert msgs == ["BBBBCC"]

    def test_invalid_byte_inside_message_discards_partial(self):
        data = b"*AA\x00*CCDD;"
        buf = bytearray()
        msgs = parse_tcp_stream(data, buf)
        assert msgs == ["CCDD"]

    def test_empty_data(self):
        buf = bytearray()
        assert parse_tcp_stream(b"", buf) == []

    def test_star_without_semicolon_leaves_buf_empty(self):
        buf = bytearray()
        msgs = parse_tcp_stream(b"*", buf)
        assert msgs == []
        assert len(buf) == 0

    def test_semicolon_with_empty_buf_skipped(self):
        buf = bytearray()
        msgs = parse_tcp_stream(b";", buf)
        assert msgs == []

    def test_all_valid_hex_chars_accepted(self):
        hex_str = "0123456789ABCDEF"
        data = f"*{hex_str};".encode("ascii")
        buf = bytearray()
        msgs = parse_tcp_stream(data, buf)
        assert msgs == [hex_str]

    def test_real_adsb_message_format(self):
        """A real DF17 message: 28-byte (56 hex char) Mode-S payload with
        the newline suffix readsb actually sends."""
        raw = "8D4840D6202CC371C32CE0576098"
        data = f"*{raw};\n".encode()
        buf = bytearray()
        msgs = parse_tcp_stream(data, buf)
        assert msgs == [raw.upper()]
