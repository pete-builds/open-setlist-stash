"""Settings-level policy tests (pure, no DB, no network)."""

from __future__ import annotations

from setlist_stash.config import Settings


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
