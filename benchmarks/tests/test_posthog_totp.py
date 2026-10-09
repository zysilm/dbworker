"""Real pinned setup-form/model checks in an isolated Python interpreter."""
import os
import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


class PostHogTOTPSetupTests(unittest.TestCase):
    def test_original_setup_persistence_and_adversarial_devices(self):
        interpreter = os.environ.get("POSTHOG_TOTP_TEST_PYTHON", sys.executable)
        probe = subprocess.run([interpreter, "-c",
            "from importlib.metadata import version; "
            "assert version('Django') == '5.2.17'; "
            "assert version('django-otp') == '1.6.0'; "
            "assert version('django-two-factor-auth') == '1.18.1'"],
            capture_output=True, text=True)
        if probe.returncode:
            if "POSTHOG_TOTP_TEST_PYTHON" in os.environ:
                self.fail(probe.stderr)
            self.skipTest("Run with the pinned PostHog TOTP dependencies")
        script = r'''
from django.conf import settings
settings.configure(SECRET_KEY='isolated-native-totp-test', USE_TZ=True,
 DATABASES={'default': {'ENGINE': 'django.db.backends.sqlite3', 'NAME': ':memory:'}},
 INSTALLED_APPS=['django.contrib.auth', 'django.contrib.contenttypes',
                 'django_otp', 'django_otp.plugins.otp_totp'],
 TWO_FACTOR_TOTP_DIGITS=6, OTP_TOTP_SYNC=True)
import django
django.setup()
from django.core.management import call_command
call_command('migrate', verbosity=0)
from django.contrib.auth import get_user_model
from django_otp.oath import totp
from django_otp import login as otp_login
from django_otp.plugins.otp_totp.models import TOTPDevice
from two_factor.forms import TOTPDeviceForm
from two_factor.utils import default_device
from types import SimpleNamespace
from unittest.mock import patch
from examples.posthog_dbworker.producer import Fixture, _verify_totp_device

user = get_user_model().objects.create_user(username='native-setup-user')
secret = bytes(range(20))
fixture = Fixture(user.pk, 'unused-current', 'unused-other', secret, 'unused-csrf', 'user@benchmark.invalid')

def reject():
 try:
  _verify_totp_device(fixture)
 except AssertionError:
  return
 raise AssertionError('Malformed setup device was admitted')

def setup():
 form = TOTPDeviceForm(secret.hex(), user, data={'token': f'{totp(secret):06d}'})
 assert form.is_valid(), form.errors
 # The setup form validates the real token directly, then persists a fresh
 # device. A device login verification must not be added to the workload.
 with patch.object(TOTPDevice, 'verify_token', side_effect=AssertionError('Unexpected login verification')):
  device = form.save()
  request = SimpleNamespace(user=user, session={})
  otp_login(request, default_device(user))
 device.refresh_from_db()
 assert device.last_t == -1 and device.confirmed
 assert request.session['otp_device_id'] == device.persistent_id
 assert _verify_totp_device(fixture).pk == device.pk
 return device

reject()  # Missing real device.
device = setup()
for change in ({'confirmed': False}, {'key': 'ff' * 20}, {'last_t': 0},
               {'name': 'other'}, {'step': 60}, {'t0': 10}, {'digits': 8},
               {'tolerance': 2}, {'drift': 2}):
 original = {key: getattr(device, key) for key in change}
 TOTPDevice.objects.filter(pk=device.pk).update(**change)
 reject()
 TOTPDevice.objects.filter(pk=device.pk).update(**original)
TOTPDevice.objects.create(user=user, key=secret.hex(), name='duplicate')
reject()
TOTPDevice.objects.exclude(pk=device.pk).delete()
assert _verify_totp_device(fixture).pk == device.pk
# Invalid setup tokens must fail the original form without persisting a device.
TOTPDevice.objects.all().delete()
valid = {totp(secret, drift=offset) for offset in (-1, 0, 1)}
invalid = next(token for token in range(1000000) if token not in valid)
form = TOTPDeviceForm(secret.hex(), user, data={'token': f'{invalid:06d}'})
assert not form.is_valid() and not TOTPDevice.objects.exists()
print('Pinned native setup form, OTP session binding, and 12 adverse cases passed')
'''
        environment = os.environ.copy()
        environment["PYTHONPATH"] = str(ROOT)
        result = subprocess.run([interpreter, "-c", script], cwd=ROOT,
            env=environment, capture_output=True, text=True, timeout=60)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("12 adverse cases passed", result.stdout)


if __name__ == "__main__":
    unittest.main()
