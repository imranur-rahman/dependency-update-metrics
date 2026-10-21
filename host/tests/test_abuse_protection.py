"""Tests for the abuse protection of the Host: the per-IP limits, the queue cap, the IP
hashes, the client IP behind the proxy, CORS, and the real Turnstile check."""

from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from contract_examples import load_example
from dmhost import JobQueue, Worker, create_app
from dmhost.client_ip import parse_networks
from dmhost.limits import IpHasher, SubmitLimits
from dmhost.settings import HostSettings
from dmhost.turnstile import TurnstileVerifier

ONE_HOUR = 3600.0


class FakeClock:
    def __init__(self, now=1_000_000.0):
        self.now = now

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def done_analysis(comparison, package):
    return {"package": package}


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def queue(tmp_path, clock):
    return JobQueue(tmp_path / "jobs.db", clock=clock)


def make_app(queue, clock, limits=SubmitLimits(), **options):
    return create_app(
        queue=queue,
        analysis=done_analysis,
        turnstile_check=lambda token, remote_ip: True,
        limits=limits,
        **options,
    )


def client_at(app, ip="203.0.113.7"):
    """Return a TestClient whose requests come from the address ``ip``."""
    return TestClient(app, client=(ip, 50000))


def submit(client, package="mypkg", **headers):
    """Submit the example Comparison for ``package``. A done Job of the same Comparison gives a
    reused result, so a test that needs a new Job after a done Job uses another package."""
    body = {**load_example("create_job_request"), "packages": [package]}
    return client.post("/api/jobs", json=body, headers=headers)


def test_second_submit_while_the_first_job_is_active_is_refused(queue, clock):
    client = client_at(make_app(queue, clock))

    first = submit(client)
    second = submit(client)

    assert first.status_code == 202
    assert second.status_code == 429
    assert second.json()["detail"].startswith("Too many requests, try again in ")


def test_submits_at_the_same_time_from_one_user_get_only_one_job(queue, clock):
    client = client_at(make_app(queue, clock))

    with ThreadPoolExecutor(max_workers=8) as pool:
        codes = sorted(pool.map(lambda _: submit(client).status_code, range(8)))

    assert codes == [202] + [429] * 7


def test_reused_result_does_not_count_against_the_per_ip_limits(queue, clock):
    # One Job per hour and one active Job: only the first submit starts a Job.
    limits = SubmitLimits(max_active_jobs_per_ip=1, max_jobs_per_ip_per_window=1)
    client = client_at(make_app(queue, clock, limits=limits))
    assert submit(client).status_code == 202
    Worker(queue, done_analysis).run_once()

    reused = [submit(client).status_code for _ in range(3)]
    other = client.post(
        "/api/jobs", json={**load_example("create_job_request"), "packages": ["otherpkg"]}
    )

    # The same Comparison gives the stored result three times. A new Comparison is the
    # second Job in the hour, so the hourly limit refuses it.
    assert reused == [200, 200, 200]
    assert other.status_code == 429


def test_reused_result_is_given_while_the_user_has_an_active_job(queue, clock):
    limits = SubmitLimits(max_active_jobs_per_ip=1, max_jobs_per_ip_per_window=10)
    client = client_at(make_app(queue, clock, limits=limits))
    submit(client)
    Worker(queue, done_analysis).run_once()
    other = {**load_example("create_job_request"), "packages": ["otherpkg"]}
    assert client.post("/api/jobs", json=other).status_code == 202

    # The user has one active Job, so a new Job waits. The stored result needs no Job.
    reused = submit(client)
    third = client.post(
        "/api/jobs", json={**load_example("create_job_request"), "packages": ["third"]}
    )

    assert reused.status_code == 200
    assert third.status_code == 429


def test_user_can_submit_again_when_the_active_job_ends(queue, clock):
    client = client_at(make_app(queue, clock))
    submit(client)

    Worker(queue, done_analysis).run_once()

    assert submit(client, "otherpkg").status_code == 202


def test_active_job_of_one_user_does_not_stop_another_user(queue, clock):
    app = make_app(queue, clock)
    submit(client_at(app, "203.0.113.7"))

    assert submit(client_at(app, "198.51.100.4")).status_code == 202


def test_message_for_an_active_job_says_to_try_again_in_one_minute(queue, clock):
    client = client_at(make_app(queue, clock))
    submit(client)

    response = submit(client)

    assert response.json() == {"detail": "Too many requests, try again in 1 min"}
    assert response.headers["retry-after"] == "60"


def hourly_only(**changes):
    """Return the default limits, with no limit on active Jobs, so that the hourly limit is
    the only per-IP limit."""
    return SubmitLimits(**{"max_active_jobs_per_ip": 100, **changes})


def test_eleventh_job_in_one_hour_is_refused_with_the_minutes_until_the_next_job(queue, clock):
    client = client_at(make_app(queue, clock, limits=hourly_only()))
    assert submit(client).status_code == 202
    clock.advance(20 * 60)
    for _ in range(9):
        assert submit(client).status_code == 202

    response = submit(client)

    # The first Job leaves the one-hour window 40 minutes from now.
    assert response.status_code == 429
    assert response.json() == load_example("submit_rate_limited")


def test_hourly_limit_allows_a_new_job_when_the_oldest_job_leaves_the_window(queue, clock):
    client = client_at(make_app(queue, clock, limits=hourly_only()))
    for _ in range(10):
        submit(client)
    clock.advance(ONE_HOUR - 1)
    assert submit(client).status_code == 429

    clock.advance(1)

    assert submit(client).status_code == 202


def test_submit_when_the_queue_holds_the_maximum_of_waiting_jobs_says_the_server_is_busy(
    queue, clock
):
    app = make_app(queue, clock, limits=SubmitLimits(max_waiting_jobs=2))
    submit(client_at(app, "192.0.2.1"))
    submit(client_at(app, "192.0.2.2"))

    response = submit(client_at(app, "192.0.2.3"))

    assert response.status_code == 503
    assert response.json() == load_example("submit_server_busy")


def test_queue_cap_counts_only_waiting_jobs(queue, clock):
    app = make_app(queue, clock, limits=SubmitLimits(max_waiting_jobs=2))
    submit(client_at(app, "192.0.2.1"))
    submit(client_at(app, "192.0.2.2"))

    assert queue.claim_next() is not None

    assert submit(client_at(app, "192.0.2.3")).status_code == 202


def test_configured_limits_change_what_the_host_allows(queue, clock):
    limits = SubmitLimits(max_active_jobs_per_ip=2, max_jobs_per_ip_per_window=3)
    client = client_at(make_app(queue, clock, limits=limits))

    codes = [submit(client, f"pkg{i}").status_code for i in range(3)]
    Worker(queue, done_analysis).run_once()
    Worker(queue, done_analysis).run_once()
    codes += [submit(client, f"new{i}").status_code for i in range(2)]

    # Two active Jobs pass and the third waits. After the two end, the hourly limit of 3
    # allows one more Job.
    assert codes == [202, 202, 429, 202, 429]


def test_limits_come_from_environment_values():
    settings = HostSettings.from_env(
        {
            "DM_HOST_MAX_ACTIVE_JOBS_PER_IP": "2",
            "DM_HOST_MAX_JOBS_PER_IP_PER_HOUR": "30",
            "DM_HOST_MAX_WAITING_JOBS": "50",
        }
    )

    assert settings.limits == SubmitLimits(
        max_active_jobs_per_ip=2, max_jobs_per_ip_per_window=30, max_waiting_jobs=50
    )


def test_default_limits_are_one_active_job_ten_jobs_an_hour_and_twenty_waiting_jobs():
    assert HostSettings.from_env({}).limits == SubmitLimits(
        max_active_jobs_per_ip=1,
        max_jobs_per_ip_per_window=10,
        max_waiting_jobs=20,
        window_seconds=ONE_HOUR,
    )


def queue_file_bytes(queue):
    """Return the bytes of the queue file and of its WAL files."""
    paths = [queue.db_path] + [
        queue.db_path.with_name(queue.db_path.name + suffix) for suffix in ("-wal", "-shm")
    ]
    return b"".join(path.read_bytes() for path in paths if path.exists())


def test_host_never_stores_the_plain_ip_address(queue, clock):
    app = make_app(queue, clock, limits=SubmitLimits())
    client = client_at(app, "203.0.113.77")

    submit(client)
    submit(client)

    assert b"203.0.113.77" not in queue_file_bytes(queue)


def test_host_removes_the_ip_hash_after_the_rate_limit_window(queue, clock):
    hasher = IpHasher(b"test key")
    app = make_app(queue, clock, limits=SubmitLimits(), ip_hasher=hasher)
    submit(client_at(app, "203.0.113.77"))
    Worker(queue, done_analysis).run_once()
    stored = hasher("203.0.113.77").encode()
    assert stored in queue_file_bytes(queue)

    clock.advance(ONE_HOUR + 1)
    queue.forget_old_submitters(ONE_HOUR)

    assert stored not in queue_file_bytes(queue)


def test_hash_of_a_job_that_is_still_active_stays_until_the_job_ends(queue, clock):
    hasher = IpHasher(b"test key")
    app = make_app(queue, clock, limits=SubmitLimits(), ip_hasher=hasher)
    client = client_at(app, "203.0.113.77")
    submit(client)
    clock.advance(ONE_HOUR + 1)

    assert queue.forget_old_submitters(ONE_HOUR) == 0
    assert submit(client).status_code == 429


def test_same_ip_gets_the_same_hash_and_other_keys_give_other_hashes():
    assert IpHasher(b"a")("192.0.2.1") == IpHasher(b"a")("192.0.2.1")
    assert IpHasher(b"a")("192.0.2.1") != IpHasher(b"a")("192.0.2.2")
    assert IpHasher(b"a")("192.0.2.1") != IpHasher(b"b")("192.0.2.1")


# --- The client IP behind Caddy ---

CADDY = "10.0.0.2"


def app_behind_caddy(queue, clock, seen_ips):
    def record_ip(token, remote_ip):
        seen_ips.append(remote_ip)
        return True

    return create_app(
        queue=queue,
        analysis=done_analysis,
        turnstile_check=record_ip,
        limits=SubmitLimits(),
        trusted_proxies=parse_networks(["10.0.0.0/8"]),
    )


def test_behind_the_trusted_proxy_the_user_address_comes_from_x_forwarded_for(queue, clock):
    seen = []
    caddy = client_at(app_behind_caddy(queue, clock, seen), CADDY)

    submit(caddy, **{"X-Forwarded-For": "203.0.113.9"})
    second_user = submit(caddy, **{"X-Forwarded-For": "198.51.100.1"})

    assert seen == ["203.0.113.9", "198.51.100.1"]
    assert second_user.status_code == 202


def test_user_cannot_choose_the_address_with_a_false_x_forwarded_for(queue, clock):
    seen = []
    caddy = client_at(app_behind_caddy(queue, clock, seen), CADDY)

    # Caddy puts the true address at the end, after the values that the user sent.
    submit(caddy, **{"X-Forwarded-For": "192.0.2.50, 203.0.113.9"})
    again = submit(caddy, **{"X-Forwarded-For": "192.0.2.51, 203.0.113.9"})

    assert seen == ["203.0.113.9", "203.0.113.9"]
    assert again.status_code == 429


def test_x_forwarded_for_from_a_peer_that_is_not_a_trusted_proxy_is_ignored(queue, clock):
    seen = []
    direct = client_at(app_behind_caddy(queue, clock, seen), "203.0.113.9")

    submit(direct, **{"X-Forwarded-For": "192.0.2.50"})

    assert seen == ["203.0.113.9"]


# --- CORS ---


def test_cors_preflight_from_another_origin_is_refused(queue, clock):
    app = make_app(queue, clock, allowed_origins=["https://imranur-rahman.github.io"])
    client = client_at(app)
    preflight = {"Access-Control-Request-Method": "POST"}

    allowed = client.options(
        "/api/jobs", headers={"Origin": "https://imranur-rahman.github.io", **preflight}
    )
    other = client.options("/api/jobs", headers={"Origin": "https://example.com", **preflight})

    assert allowed.status_code == 200
    assert allowed.headers["access-control-allow-origin"] == "https://imranur-rahman.github.io"
    assert other.status_code == 400
    assert "access-control-allow-origin" not in other.headers


def test_allowed_origins_come_from_an_environment_value():
    settings = HostSettings.from_env(
        {"DM_HOST_ALLOWED_ORIGINS": "https://imranur-rahman.github.io"}
    )

    assert settings.allowed_origins == ("https://imranur-rahman.github.io",)


# --- The real Turnstile check, with a fake Siteverify call ---


class FakeSiteverify:
    def __init__(self, body=None, error=None):
        self.body = body if body is not None else {"success": True}
        self.error = error
        self.calls = []

    def __call__(self, url, form):
        self.calls.append((url, dict(form)))
        if self.error:
            raise self.error
        return self.body


def test_turnstile_check_sends_the_token_secret_and_ip_to_siteverify():
    siteverify = FakeSiteverify()
    check = TurnstileVerifier("secret-key", post=siteverify)

    assert check("token-1", "203.0.113.9") is True
    assert siteverify.calls == [
        (
            "https://challenges.cloudflare.com/turnstile/v0/siteverify",
            {"secret": "secret-key", "response": "token-1", "remoteip": "203.0.113.9"},
        )
    ]


def test_turnstile_check_fails_when_siteverify_refuses_the_token():
    siteverify = FakeSiteverify({"success": False, "error-codes": ["timeout-or-duplicate"]})

    assert TurnstileVerifier("secret-key", post=siteverify)("token-1", None) is False


def test_turnstile_check_fails_when_siteverify_does_not_answer():
    siteverify = FakeSiteverify(error=OSError("connection refused"))

    assert TurnstileVerifier("secret-key", post=siteverify)("token-1", None) is False


@pytest.mark.parametrize("token", ["", "x" * 2049], ids=["empty", "too long"])
def test_turnstile_check_refuses_an_empty_or_too_long_token_without_a_call(token):
    siteverify = FakeSiteverify()

    assert TurnstileVerifier("secret-key", post=siteverify)(token, None) is False
    assert siteverify.calls == []


def test_turnstile_check_needs_a_secret_key():
    with pytest.raises(ValueError):
        TurnstileVerifier("", post=FakeSiteverify())


def test_submit_with_a_refused_turnstile_token_adds_no_job(queue, clock):
    siteverify = FakeSiteverify({"success": False, "error-codes": ["invalid-input-response"]})
    app = create_app(
        queue=queue,
        analysis=done_analysis,
        turnstile_check=TurnstileVerifier("secret-key", post=siteverify),
        limits=SubmitLimits(),
    )

    response = submit(client_at(app))

    assert response.status_code == 403
    assert queue.claim_next() is None
    assert siteverify.calls[0][1]["response"] == "example-token"
