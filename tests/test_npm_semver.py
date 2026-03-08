"""Tests for npm semver parsing."""

from dependency_metrics.resolvers import npm_semver_key


def test_npm_semver_prerelease_sorting() -> None:
    versions = [
        "0.0.0-insiders.b4008fc",
        "0.0.0",
        "0.0.1",
        "0.0.1-alpha.1",
        "v1.2.3",
        "1.2.3+build.7",
        "1.0.0",
        "1.0.0-beta",
    ]

    keys = [(npm_semver_key(v), v) for v in versions]
    keys = [item for item in keys if item[0] is not None]
    keys.sort(key=lambda item: item[0])
    ordered = [v for _, v in keys]

    assert ordered[0] == "0.0.0-insiders.b4008fc"
    assert ordered[-4] == "1.0.0-beta"
    assert ordered[-3] == "1.0.0"

    # v-prefix and build metadata should not affect ordering vs base version:
    # both v1.2.3 and 1.2.3+build.7 normalise to 1.2.3 and rank above 1.0.0.
    assert set(ordered[-2:]) == {"v1.2.3", "1.2.3+build.7"}
