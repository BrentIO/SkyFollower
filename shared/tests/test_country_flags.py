import pytest

from shared.country_flags import country_flag


class TestCountryFlag:
    def test_cayman_islands(self):
        assert country_flag("KY") == "\U0001F1F0\U0001F1FE"

    def test_united_kingdom(self):
        assert country_flag("GB") == "\U0001F1EC\U0001F1E7"

    def test_lowercase_input_is_normalized(self):
        assert country_flag("ky") == country_flag("KY")

    def test_rejects_non_two_letter_code(self):
        with pytest.raises(ValueError):
            country_flag("USA")

    def test_rejects_non_alpha_code(self):
        with pytest.raises(ValueError):
            country_flag("U1")
