"""Typed API exceptions and leak-free FastAPI exception handlers."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.exc import (
    InterfaceError,
    OperationalError,
    SQLAlchemyError,
    TimeoutError as SQLAlchemyTimeoutError,
)
from starlette.exceptions import HTTPException as StarletteHTTPException

from geosearch.api.schemas.common import ErrorBody, ErrorDetailItem, ErrorResponse

logger = logging.getLogger(__name__)

PG_SQLSTATE_QUERY_CANCELED = "57014"


class ApiError(Exception):
    """Base structured HTTP API error with safe public message."""

    def __init__(
        self,
        *,
        status_code: int,
        code: str,
        message: str,
        details: list[ErrorDetailItem] | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message
        self.details = details


class PlaceNotFoundError(ApiError):
    """Raised when a requested canonical active place does not exist."""

    def __init__(self) -> None:
        super().__init__(
            status_code=status.HTTP_404_NOT_FOUND,
            code="PLACE_NOT_FOUND",
            message="Place not found.",
        )


class InvalidQueryParameterError(ApiError):
    """Raised when a request parameter fails semantic validation."""

    def __init__(
        self,
        message: str,
        *,
        loc: list[str],
        error_type: str = "value_error",
    ) -> None:
        super().__init__(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            code="VALIDATION_ERROR",
            message="Request validation failed.",
            details=[ErrorDetailItem(loc=loc, msg=message, type=error_type)],
        )


def is_statement_timeout_error(exc: BaseException) -> bool:
    """Return True if a SQLAlchemy/DBAPI error represents a PostgreSQL query cancellation/timeout."""
    orig = getattr(exc, "orig", None)
    sqlstate = getattr(orig, "sqlstate", None)
    return sqlstate == PG_SQLSTATE_QUERY_CANCELED


def is_database_unavailable_error(exc: BaseException) -> bool:
    """Return True if an error indicates PostgreSQL connection or pool unavailability."""
    return isinstance(exc, (OperationalError, InterfaceError, SQLAlchemyTimeoutError))


def _error_json_response(
    *,
    status_code: int,
    code: str,
    message: str,
    details: list[ErrorDetailItem] | None = None,
) -> JSONResponse:
    payload = ErrorResponse(
        error=ErrorBody(code=code, message=message, details=details)
    ).model_dump(exclude_none=True)
    return JSONResponse(status_code=status_code, content=payload)


def _sanitize_validation_errors(raw_errors: Any) -> list[ErrorDetailItem]:
    sanitized: list[ErrorDetailItem] = []
    for err in raw_errors:
        loc_raw = err.get("loc", ())
        loc = [str(part) for part in loc_raw]
        msg = str(err.get("msg", "Invalid value."))
        err_type = str(err.get("type", "value_error"))
        sanitized.append(ErrorDetailItem(loc=loc, msg=msg, type=err_type))
    return sanitized


def register_exception_handlers(app: FastAPI) -> None:
    """Register consistent, leak-free JSON error handlers on the FastAPI application."""

    @app.exception_handler(ApiError)
    def handle_api_error(_request: Request, exc: ApiError) -> JSONResponse:
        return _error_json_response(
            status_code=exc.status_code,
            code=exc.code,
            message=exc.message,
            details=exc.details,
        )

    @app.exception_handler(RequestValidationError)
    def handle_request_validation_error(
        _request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        return _error_json_response(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            code="VALIDATION_ERROR",
            message="Request validation failed.",
            details=_sanitize_validation_errors(exc.errors()),
        )

    @app.exception_handler(StarletteHTTPException)
    def handle_http_exception(_request: Request, exc: StarletteHTTPException) -> JSONResponse:
        code_by_status = {
            status.HTTP_404_NOT_FOUND: "NOT_FOUND",
            status.HTTP_405_METHOD_NOT_ALLOWED: "METHOD_NOT_ALLOWED",
            status.HTTP_400_BAD_REQUEST: "BAD_REQUEST",
        }
        code = code_by_status.get(exc.status_code, f"HTTP_{exc.status_code}")
        message = exc.detail if isinstance(exc.detail, str) else "Request failed."
        return _error_json_response(
            status_code=exc.status_code,
            code=code,
            message=message,
        )

    @app.exception_handler(SQLAlchemyError)
    def handle_sqlalchemy_error(_request: Request, exc: SQLAlchemyError) -> JSONResponse:
        if is_statement_timeout_error(exc):
            logger.warning("Database query timed out during API request.")
            return _error_json_response(
                status_code=status.HTTP_504_GATEWAY_TIMEOUT,
                code="DATABASE_TIMEOUT",
                message="Database query timed out.",
            )
        if is_database_unavailable_error(exc):
            logger.error("Database connection unavailable during API request.")
            return _error_json_response(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                code="DATABASE_UNAVAILABLE",
                message="Database service is temporarily unavailable.",
            )
        logger.exception("Unhandled database error during API request.")
        return _error_json_response(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            code="INTERNAL_SERVER_ERROR",
            message="An unexpected internal error occurred.",
        )

    @app.exception_handler(Exception)
    def handle_unexpected_error(_request: Request, _exc: Exception) -> JSONResponse:
        logger.exception("Unhandled server error during API request.")
        return _error_json_response(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            code="INTERNAL_SERVER_ERROR",
            message="An unexpected internal error occurred.",
        )
