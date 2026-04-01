import pytest
from datastream.exceptions import DatastreamAPIError, DatastreamError


def test_base_error_is_exception():
    assert issubclass(DatastreamError, Exception)


def test_api_error_inherits_base():
    assert issubclass(DatastreamAPIError, DatastreamError)


def test_api_error_attrs():
    err = DatastreamAPIError(status_code=400, detail="bad request")
    assert err.status_code == 400
    assert err.detail == "bad request"


def test_api_error_message():
    err = DatastreamAPIError(status_code=500, detail="internal error")
    assert "500" in str(err)
    assert "internal error" in str(err)


def test_api_error_catchable_as_base():
    with pytest.raises(DatastreamError):
        raise DatastreamAPIError(status_code=422, detail="unprocessable")
