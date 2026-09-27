from datetime import date
from pathlib import Path

import pytest
import requests

from SILO.silo_notebooks import silo_utils
from SILO.silo_notebooks.constants import SILO_MAX_LAT, SILO_MAX_LON, SILO_MIN_LAT, SILO_MIN_LON
from SILO.silo_notebooks.silo_utils import (
    RETRY_DELAY_SCHEDULE,
    SILO_API_PASSWORD,
    SILO_REQUEST_COMMENT_CODE,
    _validate_csv_response_body,
    fetch_silo_csv,
    get_silo_email,
    parse_silo_csv,
    read_silo_places_csv,
)

# =============================================================================
# HELPERS
# =============================================================================

# Trimmed SILO DataDrill CSV response (format=csv) in the layout the API returns: values padded with spaces,
# a metadata column, and an undated row appended because SILO has more metadata entries than days to put them on.
_SILO_CSV_RESPONSE: str = (
    "latitude,longitude,YYYY-MM-DD,daily_rain,daily_rain_source,max_temp,max_temp_source,"
    "min_temp,min_temp_source,vp,vp_source,radiation,radiation_source,metadata\n"
    ' -32.2500, 148.6000,1990-01-01,    0.0,25, 33.5,25, 18.0,25, 19.0,25, 29.0,42,"elevation= 274.3 m"\n'
    ' -32.2500, 148.6000,1990-01-02,    4.2,25, 30.1,25, 17.5,25, 20.1,25, 24.3,42,"dataset=BoM Only"\n'
    " -32.2500, 148.6000" + "," * 12 + '"extracted=20260927"\n'
)


class _FakeWidgets:
    def __init__(self, values: dict[str, str]) -> None:
        self._values = values

    def get(self, name: str) -> str:
        return self._values[name]


class _FakeDbutils:
    def __init__(self, values: dict[str, str]) -> None:
        self.widgets = _FakeWidgets(values)


class _FakeResponse:
    def __init__(self, status_code: int, text: str = "") -> None:
        self.status_code = status_code
        self.text = text


@pytest.fixture
def sleeps(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """Records the retry backoff delays instead of sleeping."""
    recorded_sleeps: list[int] = []
    monkeypatch.setattr(silo_utils.time, "sleep", recorded_sleeps.append)
    return recorded_sleeps


def _stub_requests_get(
    monkeypatch: pytest.MonkeyPatch, outcomes: list[_FakeResponse | Exception]
) -> list[dict[str, str]]:
    """Replaces requests.get with a stub that returns or raises outcomes in order. Returns each call's params."""
    request_params: list[dict[str, str]] = []
    remaining_outcomes = iter(outcomes)

    def fake_get(url: str, params: dict[str, str], timeout: int) -> _FakeResponse:
        request_params.append(params)
        outcome: _FakeResponse | Exception = next(remaining_outcomes)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(silo_utils.requests, "get", fake_get)
    return request_params


def _fetch_two_days() -> str:
    return fetch_silo_csv(-32.25, 148.6, date(1990, 1, 1), date(1990, 1, 2), "someone@example.com")


# =============================================================================
# PLACES CSV
# =============================================================================


def test_committed_places_csv_has_fifty_unique_places_within_silo_bounds() -> None:
    places_df = read_silo_places_csv()

    assert len(places_df) == 50
    assert places_df["place_id"].is_unique
    assert places_df.notna().all().all()
    assert places_df["latitude"].between(SILO_MIN_LAT, SILO_MAX_LAT).all()
    assert places_df["longitude"].between(SILO_MIN_LON, SILO_MAX_LON).all()


def test_read_silo_places_csv_coerces_unparseable_coordinates_to_nan(tmp_path: Path) -> None:
    csv_path = tmp_path / "places.csv"
    csv_path.write_text("place_id,place_name,state,latitude,longitude\n p1 , Place One ,NSW,not-a-number,148.6\n")

    places_df = read_silo_places_csv(csv_path)

    assert places_df.loc[0, "place_id"] == "p1"
    assert places_df.loc[0, "place_name"] == "Place One"
    assert places_df["latitude"].isna().all()
    assert places_df.loc[0, "longitude"] == pytest.approx(148.6)


def test_read_silo_places_csv_rejects_missing_columns(tmp_path: Path) -> None:
    csv_path = tmp_path / "places.csv"
    csv_path.write_text("place_id,place_name,latitude\np1,Place One,-32.0\n")

    with pytest.raises(ValueError, match="state"):
        read_silo_places_csv(csv_path)


# =============================================================================
# SILO API EMAIL
# =============================================================================


def test_get_silo_email_returns_stripped_email() -> None:
    assert get_silo_email(_FakeDbutils({"silo_email": "  someone@example.com "})) == "someone@example.com"


@pytest.mark.parametrize("invalid_email", ["", "not-an-email", "someone@", "some one@example.com"])
def test_get_silo_email_rejects_invalid_values(invalid_email: str) -> None:
    with pytest.raises(ValueError, match="silo_email"):
        get_silo_email(_FakeDbutils({"silo_email": invalid_email}))


# =============================================================================
# API FETCH
# =============================================================================


def test_fetch_silo_csv_sends_the_expected_request(monkeypatch: pytest.MonkeyPatch, sleeps: list[int]) -> None:
    request_params = _stub_requests_get(monkeypatch, [_FakeResponse(200, _SILO_CSV_RESPONSE)])

    assert _fetch_two_days() == _SILO_CSV_RESPONSE
    assert request_params == [
        {
            "lat": "-32.25",
            "lon": "148.60",
            "start": "19900101",
            "finish": "19900102",
            "format": "csv",
            "comment": SILO_REQUEST_COMMENT_CODE,
            "username": "someone@example.com",
            "password": SILO_API_PASSWORD,
        }
    ]
    assert sleeps == []


def test_fetch_silo_csv_retries_server_errors(monkeypatch: pytest.MonkeyPatch, sleeps: list[int]) -> None:
    request_params = _stub_requests_get(monkeypatch, [_FakeResponse(503), _FakeResponse(200, _SILO_CSV_RESPONSE)])

    assert _fetch_two_days() == _SILO_CSV_RESPONSE
    assert len(request_params) == 2
    assert sleeps == RETRY_DELAY_SCHEDULE[:1]


def test_fetch_silo_csv_fails_fast_on_client_errors(monkeypatch: pytest.MonkeyPatch, sleeps: list[int]) -> None:
    request_params = _stub_requests_get(monkeypatch, [_FakeResponse(403, "Forbidden")])

    with pytest.raises(requests.HTTPError, match="HTTP 403"):
        _fetch_two_days()
    assert len(request_params) == 1
    assert sleeps == []


def test_fetch_silo_csv_raises_the_last_error_when_retries_run_out(
    monkeypatch: pytest.MonkeyPatch, sleeps: list[int]
) -> None:
    attempt_count: int = len(RETRY_DELAY_SCHEDULE) + 1
    request_params = _stub_requests_get(monkeypatch, [requests.ConnectionError("unreachable")] * attempt_count)

    with pytest.raises(requests.ConnectionError, match="unreachable"):
        _fetch_two_days()
    assert len(request_params) == attempt_count
    assert sleeps == RETRY_DELAY_SCHEDULE


def test_fetch_silo_csv_rejects_a_body_that_is_not_silo_csv(
    monkeypatch: pytest.MonkeyPatch, sleeps: list[int]
) -> None:
    _stub_requests_get(monkeypatch, [_FakeResponse(200, "Sorry, your request could not be processed.")])

    with pytest.raises(ValueError, match="expected SILO CSV headers"):
        _fetch_two_days()


# =============================================================================
# SILO CSV RESPONSE
# =============================================================================


def test_validate_csv_response_body_accepts_valid_csv() -> None:
    _validate_csv_response_body(_SILO_CSV_RESPONSE, -32.25, 148.6)


@pytest.mark.parametrize(
    "response_text",
    [
        "Sorry, your request could not be processed.",
        "# comment only\n\n",
        "",
    ],
)
def test_validate_csv_response_body_rejects_error_responses(response_text: str) -> None:
    with pytest.raises(ValueError):
        _validate_csv_response_body(response_text, -32.25, 148.6)


def test_parse_silo_csv_renames_columns_and_injects_key() -> None:
    parsed_df = parse_silo_csv(_SILO_CSV_RESPONSE, "-32.25,148.6")

    assert {"date", "lat", "lon", "silo_latlon_key"}.issubset(parsed_df.columns)
    assert not {"YYYY-MM-DD", "latitude", "longitude", "metadata"} & set(parsed_df.columns)
    assert (parsed_df["silo_latlon_key"] == "-32.25,148.6").all()


def test_parse_silo_csv_drops_undated_metadata_rows() -> None:
    parsed_df = parse_silo_csv(_SILO_CSV_RESPONSE, "-32.25,148.6")

    assert parsed_df["date"].tolist() == ["1990-01-01", "1990-01-02"]


def test_parse_silo_csv_keeps_values_exactly_as_delivered() -> None:
    first_row = parse_silo_csv(_SILO_CSV_RESPONSE, "-32.25,148.6").iloc[0]

    # Read as numbers, the undated row's empty cells would turn the source codes into floats ("25.0").
    assert (first_row["lat"], first_row["daily_rain"], first_row["daily_rain_source"]) == ("-32.2500", "0.0", "25")


def test_parse_silo_csv_turns_empty_cells_into_none() -> None:
    response_text = "latitude,longitude,YYYY-MM-DD,daily_rain,daily_rain_source\n-32.25,148.60,1990-01-01,,25\n"

    assert parse_silo_csv(response_text, "-32.25,148.6").loc[0, "daily_rain"] is None
