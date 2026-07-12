import io
import logging

from shared.logging_setup import configure_logging


class TestConfigureLogging:
    def teardown_method(self):
        logging.getLogger().handlers.clear()
        logging.getLogger().setLevel(logging.WARNING)

    def test_debug_level_sets_debug(self):
        configure_logging("debug")
        assert logging.getLogger().getEffectiveLevel() == logging.DEBUG

    def test_info_level_sets_info(self):
        configure_logging("info")
        assert logging.getLogger().getEffectiveLevel() == logging.INFO

    def test_none_defaults_to_info(self):
        configure_logging(None)
        assert logging.getLogger().getEffectiveLevel() == logging.INFO

    def test_missing_arg_defaults_to_info(self):
        configure_logging()
        assert logging.getLogger().getEffectiveLevel() == logging.INFO

    def test_unrecognized_value_defaults_to_info(self):
        configure_logging("verbose")
        assert logging.getLogger().getEffectiveLevel() == logging.INFO

    def test_debug_message_emitted_at_debug_level(self):
        stream = io.StringIO()
        configure_logging("debug", stream=stream)
        logging.getLogger("test").debug("hello")
        assert "hello" in stream.getvalue()

    def test_debug_message_suppressed_at_info_level(self):
        stream = io.StringIO()
        configure_logging("info", stream=stream)
        logging.getLogger("test").debug("hello")
        assert "hello" not in stream.getvalue()

    def test_format_includes_level_and_name(self):
        stream = io.StringIO()
        configure_logging("info", stream=stream)
        logging.getLogger("my-runner").warning("uh oh")
        output = stream.getvalue()
        assert "[WARNING]" in output
        assert "my-runner" in output
        assert "uh oh" in output

    def test_reconfigures_on_repeated_calls(self):
        stream = io.StringIO()
        configure_logging("info", stream=stream)
        configure_logging("debug", stream=stream)
        assert logging.getLogger().getEffectiveLevel() == logging.DEBUG
