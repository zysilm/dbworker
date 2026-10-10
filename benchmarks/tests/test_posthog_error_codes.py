"""Offline failure privacy and single native-dispatch seam checks."""
import sys
import types
import unittest
from unittest.mock import Mock, patch

from examples.posthog_dbworker import producer


class ErrorDetail(str):
    """Offline equivalent of the string/code interface used by DRF."""

    def __new__(cls, message, code):
        value = super().__new__(cls, message)
        value.code = code
        return value


class PostHogErrorCodeTests(unittest.TestCase):
    def test_nested_codes_exclude_private_values(self):
        data = {"private-session-key": [
            ErrorDetail("123456 user@example.invalid", "token_invalid"),
            {"code": "setup_expired", "detail": "private-token"},
            {"code": "private_session_secret"},
            {"detail": ErrorDetail("private-secret", "permission_denied")},
            {"code": "123456"}, {"code": "user@example.invalid"},
            {"code": ["private-token"]}, {"message": "setup_expired"},
            ErrorDetail("private-secret", "private_secret")],
            "user": 17, "session": "private-session", "token": "123456"}
        self.assertEqual(producer._error_codes(data),
                         ["permission_denied", "setup_expired", "token_invalid"])
        self.assertEqual(producer._error_codes({"message": "token_invalid"}), [])
        self.assertEqual(producer._error_codes([
            {"code": "token_invalid"}, {"code": "token_invalid"}]), ["token_invalid"])

    def test_failed_validation_dispatches_once_and_reports_only_codes(self):
        fixture = producer.Fixture(17, "private-session", "private-other",
                                   b"private-secret", "private-csrf", "private-user@example.invalid")
        response = types.SimpleNamespace(status_code=400, data={
            "token": [ErrorDetail("123456 private-session private-user@example.invalid", "token_invalid")],
            "detail": "private-secret"})
        session = Mock()
        request = types.SimpleNamespace(COOKIES={})
        factory = types.SimpleNamespace(post=Mock(return_value=request))
        middleware = types.SimpleNamespace(process_request=Mock())
        handler = Mock(return_value=response)
        totp = Mock(return_value=123456)
        view = Mock(return_value=handler)
        modules = {
            "django.conf": types.SimpleNamespace(settings=types.SimpleNamespace(CSRF_COOKIE_NAME="csrftoken")),
            "django.contrib.auth.middleware": types.SimpleNamespace(AuthenticationMiddleware=lambda _: middleware),
            "django_otp.oath": types.SimpleNamespace(totp=totp),
            "posthog.api.user": types.SimpleNamespace(UserViewSet=types.SimpleNamespace(as_view=view)),
            "posthog.session.backend": types.SimpleNamespace(SessionStore=lambda **_: session),
            "rest_framework.test": types.SimpleNamespace(APIRequestFactory=lambda **_: factory),
        }
        with patch.dict(sys.modules, modules), patch.object(producer, "verify") as verifier:
            with self.assertRaises(AssertionError) as caught:
                producer.validate(fixture)
        self.assertEqual(str(caught.exception),
                         "Original 2FA validation API failed: HTTP 400; error_codes=['token_invalid']")
        handler.assert_called_once_with(request, uuid="@me")
        view.assert_called_once_with({"post": "two_factor_validate"})
        totp.assert_called_once_with(fixture.secret)
        session.save.assert_not_called()
        verifier.assert_not_called()
        for private in ("123456", "private-session", "private-user", "private-secret", "private-csrf"):
            self.assertNotIn(private, str(caught.exception))


if __name__ == "__main__":
    unittest.main()
