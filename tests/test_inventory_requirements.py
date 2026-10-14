import unittest

from s3_copy_desktop_app.app import AUDIT_ENDPOINT_REQUIREMENTS, S3CopyApp


class InventoryRequirementsTests(unittest.TestCase):
    def test_unrequired_content_is_not_applicable(self):
        for endpoint, content_type in (
            ("amazon", "Series"), ("t_plus", "Series"),
            ("youtube", "Season"), ("roku", "Season"), ("frndly", "Season"),
        ):
            with self.subTest(endpoint=endpoint, content_type=content_type):
                self.assertEqual(S3CopyApp._audit_status_for_requirement(
                    AUDIT_ENDPOINT_REQUIREMENTS[endpoint][content_type], {}, {}), "N/A")

    def test_optional_background_does_not_block_completion(self):
        requirement = {"art": ("bg_2x3", "ca_7x3")}
        self.assertEqual(S3CopyApp._audit_status_for_requirement(
            requirement, {}, {"ca_7x3": "3450x1100"}), "complete")

    def test_caption_rules_and_video_fallback(self):
        for endpoint, types in AUDIT_ENDPOINT_REQUIREMENTS.items():
            required = S3CopyApp._audit_requirement_fields(types["Episode"])
            self.assertIn("srt" if endpoint == "amazon" else "vtt", required)
            self.assertNotIn("vtt" if endpoint == "amazon" else "srt", required)
        self.assertEqual(S3CopyApp._audit_status_for_fields(
            ("mov", "srt"), {"mov": "mp4", "srt": "present"}, {}), "complete")

    def test_art_extensions(self):
        self.assertIsNone(S3CopyApp._extract_audit_art_info("eng_tt_9x5_1800x1000.jpg"))
        self.assertIsNone(S3CopyApp._extract_audit_art_info("eng_ca_7x3_3450x1100.png"))
        self.assertIsNotNone(S3CopyApp._extract_audit_art_info("eng_tt_9x5_1800x1000.png"))

    def test_custom_requirements(self):
        fields = S3CopyApp._normalize_custom_audit_requirements(("srt", "ca_7x3", "bg_2x3"))
        self.assertEqual(fields, ("srt", "ca_7x3"))
        self.assertEqual(S3CopyApp._audit_status_for_fields(fields, {"srt": "present"}, {}), "incomplete")


if __name__ == "__main__":
    unittest.main()
