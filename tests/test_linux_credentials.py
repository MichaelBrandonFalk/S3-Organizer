import unittest
from unittest.mock import patch

from s3_copy_desktop_app import credentials_store as store


class LinuxCredentialTests(unittest.TestCase):
    def test_linux_backend_failure_never_writes_to_fallback(self):
        credentials = store.AwsCredentials("test-id", "test-secret")
        with patch.object(store.sys, "platform", "linux"), \
                patch.dict("sys.modules", {"keyring.backends.SecretService": None}), \
                patch.object(store.keyring, "set_password") as write:
            with self.assertRaisesRegex(RuntimeError, "session-only"):
                store.save_credentials(credentials)
            write.assert_not_called()


if __name__ == "__main__":
    unittest.main()
