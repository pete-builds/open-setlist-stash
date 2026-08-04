"""Settings-level policy tests (pure, no DB, no network)."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from setlist_stash.config import DEV_SESSION_SECRET, Settings


class TestDefaultSessionSecretIsFailClosed:
    """The shipped signing key must not survive contact with production.

    SESSION_SECRET signs the identity cookie. Its default is published in this
    repo and baked into every ghcr.io image, so keeping it on a public
    deployment means anyone can forge a session for any user id, silently.
    """

    def test_https_base_url_with_the_default_secret_refuses_to_construct(
        self,
    ) -> None:
        with pytest.raises(ValidationError) as excinfo:
            Settings(base_url="https://picks.example.com")
        assert "SESSION_SECRET" in str(excinfo.value)
        assert "BASE_URL is https" in str(excinfo.value)

    def test_cookie_secure_with_the_default_secret_refuses_to_construct(
        self,
    ) -> None:
        with pytest.raises(ValidationError) as excinfo:
            Settings(cookie_secure=True)
        assert "COOKIE_SECURE=true" in str(excinfo.value)

    def test_error_names_both_signals_when_both_are_set(self) -> None:
        with pytest.raises(ValidationError) as excinfo:
            Settings(cookie_secure=True, base_url="https://picks.example.com")
        message = str(excinfo.value)
        assert "COOKIE_SECURE=true and BASE_URL is https" in message

    def test_a_real_secret_boots_fine_on_https(self) -> None:
        cfg = Settings(
            session_secret="a-real-long-random-value",  # type: ignore[arg-type]
            cookie_secure=True,
            base_url="https://picks.example.com",
        )
        assert cfg.cookie_secure is True

    def test_local_http_development_is_untouched(self) -> None:
        # The whole point: `docker compose up` out of the box still works.
        cfg = Settings(base_url="http://localhost:3706")
        assert cfg.session_secret.get_secret_value() == DEV_SESSION_SECRET

    def test_the_check_is_case_insensitive_on_the_scheme(self) -> None:
        with pytest.raises(ValidationError):
            Settings(base_url="HTTPS://picks.example.com")


class TestSiteDescription:
    """The social-preview blurb must never name a band the deployment lacks.

    The base template used to hardcode a Phish sentence, so every tenant of the
    band-agnostic platform served ``og:description="Phish setlist picks game"``
    to social crawlers. The derivation below is what replaced it.
    """

    def test_explicit_value_wins(self) -> None:
        cfg = Settings(
            site_description="Custom blurb.",
            mcp_subject="Umphrey's McGee",
            site_name="Wappy Picks",
        )
        assert cfg.site_description_effective == "Custom blurb."

    def test_derives_from_mcp_subject(self) -> None:
        cfg = Settings(mcp_subject="Umphrey's McGee", site_name="Wappy Picks")
        assert cfg.site_description_effective.startswith("Umphrey's McGee setlist")
        assert "Phish" not in cfg.site_description_effective

    def test_falls_back_to_site_name_without_a_subject(self) -> None:
        cfg = Settings(site_name="Some Other Picks")
        effective = cfg.site_description_effective
        assert effective.startswith("Some Other Picks:")
        assert "Phish" not in effective

    def test_default_deployment_names_no_band(self) -> None:
        cfg = Settings()
        effective = cfg.site_description_effective
        assert effective
        assert "Phish" not in effective
        assert "Umphrey" not in effective
