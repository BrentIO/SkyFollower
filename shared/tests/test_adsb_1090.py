from shared.adsb_1090 import parse_tcp_stream


class TestParseTcpStream:
    """Tests for parse_tcp_stream — the readsb raw-format (*hex;) parser
    shared by the receiver and the traffic-recorder tool."""

    def test_single_complete_message(self):
        """A complete *HEXDATA; produces one message."""
        data = b"*8D4B1900EA11DA58A9123456;"
        buf = bytearray()
        msgs = parse_tcp_stream(data, buf)
        assert msgs == ["8D4B1900EA11DA58A9123456"]

    def test_multiple_messages_in_one_chunk(self):
        """Two complete messages in one chunk produce two results."""
        data = b"*AABBCC;*DDEEFF;"
        buf = bytearray()
        msgs = parse_tcp_stream(data, buf)
        assert msgs == ["AABBCC", "DDEEFF"]

    def test_lowercase_hex_normalised_to_upper(self):
        """Lowercase hex bytes are uppercased in the output."""
        data = b"*aabbcc;"
        buf = bytearray()
        msgs = parse_tcp_stream(data, buf)
        assert msgs == ["AABBCC"]

    def test_message_split_across_chunks(self):
        """A message split at a chunk boundary is correctly reassembled."""
        buf = bytearray()
        msgs1 = parse_tcp_stream(b"*AABB", buf)
        assert msgs1 == []
        msgs2 = parse_tcp_stream(b"CC;", buf)
        assert msgs2 == ["AABBCC"]

    def test_newline_between_messages_ignored(self):
        """Newlines and other non-hex bytes outside a message are benign."""
        data = b"*AABBCC;\n*DDEEFF;\n"
        buf = bytearray()
        msgs = parse_tcp_stream(data, buf)
        assert msgs == ["AABBCC", "DDEEFF"]

    def test_star_resets_partial_buffer(self):
        """A new '*' discards any previously accumulated partial bytes."""
        buf = bytearray()
        # Start accumulating but then get a new star before the semicolon
        parse_tcp_stream(b"*AAAA", buf)
        msgs = parse_tcp_stream(b"*BBBBCC;", buf)
        # Only the second (complete) message should be emitted
        assert msgs == ["BBBBCC"]

    def test_invalid_byte_inside_message_discards_partial(self):
        """A non-hex byte inside a message resets the buffer silently."""
        # 0x00 is not a hex char; the partial 'AA' is discarded and 'CCDD' emitted
        data = b"*AA\x00*CCDD;"
        buf = bytearray()
        msgs = parse_tcp_stream(data, buf)
        assert msgs == ["CCDD"]

    def test_empty_data(self):
        """Empty byte string produces no messages."""
        buf = bytearray()
        assert parse_tcp_stream(b"", buf) == []

    def test_star_without_semicolon_leaves_buf_empty(self):
        """A lone '*' with no following ';' leaves buf empty (star resets buf)."""
        buf = bytearray()
        msgs = parse_tcp_stream(b"*", buf)
        assert msgs == []
        assert len(buf) == 0

    def test_semicolon_with_empty_buf_skipped(self):
        """A ';' when buf is empty doesn't produce an empty string."""
        buf = bytearray()
        msgs = parse_tcp_stream(b";", buf)
        assert msgs == []

    def test_all_valid_hex_chars_accepted(self):
        """All 16 hex digit characters (0-9, A-F) are accepted."""
        hex_str = "0123456789ABCDEF"
        data = f"*{hex_str};".encode("ascii")
        buf = bytearray()
        msgs = parse_tcp_stream(data, buf)
        assert msgs == [hex_str]

    def test_real_adsb_message_format(self):
        """
        Real ADS-B DF17 message with newline suffix (as readsb actually sends).
        The hex part is a valid 28-byte (56 hex char) Mode-S message.
        """
        raw = "8D4840D6202CC371C32CE0576098"
        data = f"*{raw};\n".encode()
        buf = bytearray()
        msgs = parse_tcp_stream(data, buf)
        assert msgs == [raw.upper()]
