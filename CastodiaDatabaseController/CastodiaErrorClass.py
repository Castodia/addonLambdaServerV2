import traceback

from psycopg2.errors import DataError, InvalidTextRepresentation
# from sentry_sdk import capture_exception
# from werkzeug.exceptions import HTTPException

"""
Errors
"""

class ClientException(Exception):
    pass

class CastodiaError(Exception):
    """Generic CastodiaError which should be the base of all errors thrown by API"""

    def __init__(
        self, message, error_code, status_code, absorbed=None
    ):  # use extra to absorb other errors for logs
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.error_code = error_code
        self.absorbed = absorbed

    def __str__(self):
        return "Error Code: '%s' || Status: '%s' || Message: '%s'" % (
            self.error_code,
            self.status_code,
            self.message,
        )

    def get_log(self):
        absorbed_is_castodia_error = isinstance(self.absorbed, CastodiaError)
        return "castodialogs: Error Code: '%s' || Status: '%s' || Message: '%s' || Traceback: '%s'" % (
            self.error_code,
            self.status_code,
            self.message + "; Absorbed: " + self.absorbed.get_log()
            if absorbed_is_castodia_error
            else str(self.absorbed),
            self._get_trace() + "\n" + self.absorbed._get_trace()
            if absorbed_is_castodia_error
            else self._get_trace(error=self.absorbed),
        )

    def _get_trace(self, full_trace=False, error=None):
        if not error:
            error = self
        tb = traceback.format_exception(
            etype=type(error), value=error, tb=error.__traceback__
        )
        if not full_trace and len(tb) >= 6:
            return "".join(tb[-6:])
        return "".join(tb)

    def get_response(self):
        return {
            "message": self.message,
            "qewqweqweqw": self.message,
            # "data": None,
            # "error_code": self.error_code,
            "errorType": self.error_code,
        }, self.status_code

    # TODO
    def send_analytics(self):
        return


class AuthError(CastodiaError):
    """AuthError should be thrown when there is a problem with authentication"""

    def __init__(self, error_code, message, status_code, absorbed=None):
        super().__init__(message, error_code, status_code, absorbed=absorbed)


class ResourceNotFound(CastodiaError):
    """ResourceNotFound should be thrown when a resource is not found; returns 404 status code"""

    def __init__(self, resource_type, absorbed=None):
        super().__init__(
            resource_type.title() + " not found", "not_found", 404, absorbed=absorbed
        )


class NotAuthorized(CastodiaError):
    """NotAuthorized should be thrown when a user is not authorized to access a resource; returns 403 status code
    `access_type` is the type of access attempted (read, delete etc.)"""

    def __init__(self, resource_type, access_type, absorbed=None):
        super().__init__(
            f"User is not authorized to {access_type} {resource_type}",
            "forbidden",
            403,
            absorbed=absorbed,
        )


class ResourceGetError(CastodiaError):
    """`ResourceGetError` should be thrown when there is an error other than `ResourceNotFound`
    when fetching a resource"""

    def __init__(self, resource_type, absorbed=None):
        super().__init__(
            f"Error fetching {resource_type}",
            "get_resource_failed",
            500,
            absorbed=absorbed,
        )


class ResourceCreationError(CastodiaError):
    """ResourceCreationError should be thrown when there is an error creating a resource"""

    def __init__(self, resource_type, absorbed=None):
        super().__init__(
            f"Error creating {resource_type}",
            "create_resource_failed",
            500,
            absorbed=absorbed,
        )


class ResourceUpdateError(CastodiaError):
    """ResourceUpdateError should be thrown when there is an error updating a resource"""

    def __init__(self, resource_type, absorbed=None):
        super().__init__(
            f"Error updating {resource_type}",
            "update_resource_failed",
            500,
            absorbed=absorbed,
        )


class ResourceDeleteError(CastodiaError):
    """ResourceDeleteError should be thrown when there is an error deleting a resource"""

    def __init__(self, resource_type, absorbed=None):
        super().__init__(
            f"Error deleting {resource_type}",
            "delete_resource_failed",
            500,
            absorbed=absorbed,
        )


class ResourceExistsError(CastodiaError):
    def __init__(self, resource_type, absorbed=None):
        super().__init__(
            f"{resource_type} already exists! Please use another identifier.",
            "resource_exists",
            409,
            absorbed=absorbed,
        )


class UnknownError(CastodiaError):
    def __init__(self, message="", absorbed=None):
        super().__init__(
            "An unknown error occurred " + message,
            "unknown_error",
            500,
            absorbed=absorbed,
        )


class FormatError(CastodiaError):
    def __init__(self, message, absorbed=None):
        super().__init__(message, "format_error", 400, absorbed=absorbed)


class ConnectionError(CastodiaError):
    def __init__(self, message, absorbed=None):
        super().__init__(message, "connection_error", 400, absorbed=absorbed)


"""
Handlers
"""


def register_error_handlers(app):
    app.register_error_handler(Exception, handle_exception)
    app.register_error_handler(AuthError, handle_auth_error)
    app.register_error_handler(CastodiaError, handle_castodia_error)


def handle_exception(e):
    """Return JSON instead of HTML for errors."""
    # if isinstance(e, HTTPException):
    #     return {"error_code": e.name, "data": None, "message": e.description}, e.code

    print("castodialogs: Non-HTTP Error occurred: " + type(e).__name__ + ": " + str(e))
    if isinstance(e, InvalidTextRepresentation):
        return (
            {
                "error_code": "invalid_text_representation",
                "data": None,
                "message": "Arguments are improperly formatted. Please check UUID format.",
            },
            400,
        )
    if isinstance(e, DataError):
        return (
            {
                "error_code": "data_error",
                "data": None,
                "message": "Issue with supplied arguments: " + str(e),
            },
            400,
        )
    tb = traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__)
    print("".join(tb))
    return (
        {
            "error_code": "unknown_error",
            "message": type(e).__name__ + ": " + str(e),
            "data": None,
        },
        500,
    )


def handle_castodia_error(e):
    print(e.get_log())
    return e.get_response()


def handle_auth_error(e):
    return e.get_response()
