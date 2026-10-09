"""Invoke the original authenticated 2FA validation handler in both arms.

The transport fixture dispatches through DRF and retains original authentication,
CSRF, permission and throttle implementations. It does not include the complete
Django middleware chain or an HTTP server. Credentials remain private fixtures.
"""
from __future__ import annotations

import contextvars
import threading
import hashlib
import json
import os
import time
from contextlib import contextmanager
from dataclasses import dataclass
from types import SimpleNamespace

PATH = "/api/users/@me/two_factor_validate/"
SETUP_KEYS = ("django_two_factor-hex", "django_two_factor-qr_secret_key")


@dataclass
class Fixture:
    user_id: int
    session_key: str
    other_session_key: str
    secret: bytes
    csrf_token: str
    recipient: str


def prepare(user):
    """Create real authenticated sessions and setup state before measurement."""
    from django.conf import settings
    from django.contrib.auth import login
    from django.http import HttpRequest
    from django.middleware.csrf import get_token
    from posthog.helpers.session_cache import SessionCache
    from posthog.session.backend import SessionStore

    def login_session():
        request = HttpRequest()
        request.META["REMOTE_ADDR"] = "127.0.0.1"
        request.session = SessionStore()
        login(request, user, backend="django.contrib.auth.backends.ModelBackend")
        request.session[settings.SESSION_COOKIE_CREATED_AT_KEY] = time.time()
        request.session[settings.SESSION_LAST_REAUTH_AT_KEY] = time.time()
        request.session.save()
        return request

    current, other = login_session(), login_session()
    secret = os.urandom(20)
    cache = SessionCache(current.session)
    cache.set(SETUP_KEYS[0], secret.hex(), timeout=3600, store_in_session=True)
    cache.set(SETUP_KEYS[1], "private-benchmark-setup-state", timeout=3600, store_in_session=True)
    current.session.save()
    get_token(current)
    return Fixture(user.pk, current.session.session_key, other.session.session_key,
                   secret, current.META["CSRF_COOKIE"], user.email)


def identity(fixture, operation_id):
    """Bind operation to its actual user/recipient without leaking session keys."""
    payload = {"operation_id": operation_id, "user_id": fixture.user_id,
               "recipient_sha256": hashlib.sha256(fixture.recipient.encode()).hexdigest()}
    return {**payload, "sha256": hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()}


_notification_context = contextvars.ContextVar("posthog_notification_publication", default=None)
_notification_lock = threading.Lock()
_notification_users = 0
_notification_original = None


@contextmanager
def route_notification(submit, expected_user_id):
    """Route concurrent producers independently without serializing native APIs."""
    global _notification_users, _notification_original
    from posthog.tasks.email import send_two_factor_auth_enabled_email
    task = send_two_factor_auth_enabled_email
    captured = []
    token = _notification_context.set((submit, expected_user_id, captured))

    def capture(args=None, kwargs=None, **options):
        context = _notification_context.get()
        if context is None:
            raise ValueError("Notification publication outside its producer context")
        callback, user_id, publications = context
        if tuple(args or ()) != (user_id,) or kwargs or options or publications:
            raise ValueError("Unreviewed or duplicate original 2FA notification publication")
        result = callback(user_id)
        publications.append(result)
        return SimpleNamespace(id=str(result))

    with _notification_lock:
        if _notification_users == 0:
            _notification_original = task.apply_async
            task.apply_async = capture
        _notification_users += 1
    try:
        yield
        if len(captured) != 1:
            raise ValueError("The validation API did not publish one notification")
    finally:
        _notification_context.reset(token)
        with _notification_lock:
            _notification_users -= 1
            if _notification_users == 0:
                task.apply_async = _notification_original
                _notification_original = None


def validate(fixture):
    """Run native request dispatch, then persist its actual session mutations."""
    from django.conf import settings
    from django.contrib.auth.middleware import AuthenticationMiddleware
    from django_otp.oath import totp
    from posthog.api.user import UserViewSet
    from posthog.session.backend import SessionStore
    from rest_framework.test import APIRequestFactory

    token = f"{totp(fixture.secret):06d}"
    request = APIRequestFactory(enforce_csrf_checks=True).post(
        PATH, {"token": token}, format="json", HTTP_X_CSRFTOKEN=fixture.csrf_token,
        HTTP_HOST="localhost", REMOTE_ADDR="127.0.0.1")
    request.COOKIES[settings.CSRF_COOKIE_NAME] = fixture.csrf_token
    request.session = SessionStore(session_key=fixture.session_key)
    AuthenticationMiddleware(lambda request: None).process_request(request)
    response = UserViewSet.as_view({"post": "two_factor_validate"})(request, uuid="@me")
    if response.status_code != 200 or response.data != {"success": True}:
        raise AssertionError(f"Original 2FA validation API failed: HTTP {response.status_code}")
    request.session.save()
    effects = verify(fixture)
    return {"status_code": response.status_code, "response": dict(response.data), "effects": effects}


def _verify_totp_device(fixture):
    """Check the original setup form's persisted device, not a later OTP login.

    Pinned two-factor 1.18.1 validates the token directly with ``oath.totp``
    and creates the device afterwards. It does not call ``verify_token``;
    django-otp 1.6.0 therefore retains the initial replay counter of -1.
    """
    from django_otp.plugins.otp_totp.models import TOTPDevice

    devices = list(TOTPDevice.objects.filter(user_id=fixture.user_id))
    if (len(devices) != 1 or not devices[0].confirmed or devices[0].key != fixture.secret.hex()
            or devices[0].name != "default" or devices[0].last_t != -1
            or devices[0].step != 30 or devices[0].t0 != 0 or devices[0].digits != 6
            or devices[0].tolerance != 1 or devices[0].drift not in (-1, 0, 1)):
        raise AssertionError("Original API did not persist its verified TOTP setup device")
    return devices[0]


def verify(fixture):
    """Require real TOTP setup, session flags/cache cleanup and revocation."""
    from posthog.helpers.session_cache import SessionCache
    from posthog.session.backend import SessionStore
    from posthog.session.models import Session

    device = _verify_totp_device(fixture)
    session = SessionStore(session_key=fixture.session_key)
    if (session.get("two_factor_verified") is not True
            or session.get("otp_device_id") != device.persistent_id):
        raise AssertionError("Original API did not persist verified session state")
    if any(SessionCache(session).exists(key) for key in SETUP_KEYS):
        raise AssertionError("Original API did not clear its setup cache and session keys")
    sessions = list(Session.objects.filter(user_id=fixture.user_id).values_list("session_key", flat=True))
    if sessions != [fixture.session_key] or Session.objects.filter(session_key=fixture.other_session_key).exists():
        raise AssertionError("Original API did not revoke the other authenticated session")
    return {"user_id": fixture.user_id, "totp_device_id": device.pk,
            "totp_verified": True, "session_verified": True, "otp_device_matches": True,
            "setup_cache_and_session_keys_removed": True, "other_session_revoked": True,
            "remaining_sessions": len(sessions)}
