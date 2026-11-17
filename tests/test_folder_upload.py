import csv
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock
from unittest.mock import patch

from s3_copy_desktop_app.app import S3CopyApp, DirectUploadItem
from s3_copy_desktop_app.s3_service import S3ObjectRef
from s3_copy_desktop_app.config_store import AppConfig
from s3_copy_desktop_app.folder_upload import (
    FolderUploadRow, destination_key, map_local_folder, parse_destination,
    scan_s3_folders, validate_selection,
)


class FolderUploadTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def add_file(self, relative):
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"test")
        return path

    def test_episode_mapping_and_file_filter(self):
        self.add_file("s01_e25/art.jpg")
        self.add_file("s01_e01/captions.vtt")
        self.add_file("results.csv")
        rows = map_local_folder(str(self.root), "series/title/s01", ["", "e01", "e25"])
        self.assertEqual({row.relative_path: row.folder for row in rows}, {
            "s01_e01/captions.vtt": "e01", "s01_e25/art.jpg": "e25"})
        self.assertTrue(all(row.selected for row in rows))
        self.assertEqual(destination_key("series/title/s01", rows[-1].folder, "art.jpg"), "series/title/s01/e25/art.jpg")

    def test_nested_tree_case_number_and_las(self):
        self.add_file("S1/E3_LAS/art.png")
        self.add_file("e04/subfolder/art.jpg")
        rows = map_local_folder(str(self.root), "series/title/s01", ["", "e03_las", "e04", "e04/subfolder"])
        self.assertEqual([row.folder for row in rows], ["e03_las", "e04/subfolder"])

    def test_unknown_and_ambiguous_folders_not_selected(self):
        self.add_file("s02_e01/art.jpg")
        self.add_file("e2/art.jpg")
        rows = map_local_folder(str(self.root), "series/title/s01", ["", "e01", "e02", "E2"])
        self.assertTrue(all(row.folder is None and not row.selected for row in rows))
        rows[0].selected = True
        with self.assertRaisesRegex(ValueError, "Choose a destination"):
            validate_selection(rows, "series/title/s01")

    def test_non_episode_folder_names_remain_case_sensitive(self):
        self.add_file("Art/file.png")
        self.assertIsNone(map_local_folder(str(self.root), "title", ["", "art"])[0].folder)

    def test_root_files_and_optional_all_types(self):
        self.add_file("results.csv")
        self.add_file(".DS_Store")
        self.add_file(".hidden/art.jpg")
        self.add_file("image.jpg")
        self.assertEqual(len(map_local_folder(str(self.root), "", [""])), 1)
        rows = map_local_folder(str(self.root), "", [""], include_all=True)
        self.assertEqual(len(rows), 2)
        self.assertTrue(all(row.folder == "" for row in rows))

    def test_symlinks_are_skipped(self):
        source = self.add_file("image.jpg")
        (self.root / "link.jpg").symlink_to(source)
        (self.root / "loop").symlink_to(self.root, target_is_directory=True)
        self.assertEqual(len(map_local_folder(str(self.root), "", [""])), 1)

    def test_duplicate_targets_block_upload(self):
        first = self.add_file("first/image.jpg")
        second = self.add_file("second/image.jpg")
        rows = [FolderUploadRow(str(first), "first/image.jpg", "e01"), FolderUploadRow(str(second), "second/image.jpg", "e01")]
        with self.assertRaisesRegex(ValueError, "same destination"):
            validate_selection(rows, "s01")
        rows[1].selected = False
        self.assertEqual(validate_selection(rows, "s01"), rows[:1])

    def test_folder_listing_uses_delimiter_and_all_pages(self):
        paginator = Mock()
        def pages(**kwargs):
            if kwargs["Prefix"] == "s01/":
                return [{"CommonPrefixes": [{"Prefix": "s01/e01/"}]}, {"CommonPrefixes": [{"Prefix": "s01/e25/"}]}]
            if kwargs["Prefix"] == "s01/e25/":
                return [{"CommonPrefixes": [{"Prefix": "s01/e25/art/"}]}]
            return [{}]
        paginator.paginate.side_effect = pages
        client = Mock()
        client.get_paginator.return_value = paginator
        self.assertEqual(scan_s3_folders(client, "bucket", "s01"), ["", "e01", "e25", "e25/art"])
        for call in paginator.paginate.call_args_list:
            self.assertEqual(call.kwargs["Delimiter"], "/")
        with self.assertRaises(InterruptedError):
            scan_s3_folders(client, "bucket", "s01", lambda: True)

    def test_destination_validation(self):
        self.assertEqual(parse_destination(" s3://bucket/series/title/s01/ "), ("bucket", "series/title/s01"))
        self.assertEqual(parse_destination("s3://bucket/"), ("bucket", ""))
        for value in ("bucket/title", "s3:///title", "s3://bucket/a//b", "s3://bucket/../title"):
            with self.assertRaises(ValueError):
                parse_destination(value)

    def test_exported_mapping_overrides_settings_and_preserves_empty_prefix(self):
        source = self.add_file("art.jpg")
        sheet = self.root / "mapping.csv"
        with sheet.open("w", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(["Title #", "Local File Path", "Desired Move Folder", "Destination Bucket", "Destination Prefix"])
            writer.writerow([1, str(source), "e25", "chosen-bucket", ""])
        app = S3CopyApp.__new__(S3CopyApp)
        app.config = AppConfig(dest_bucket="settings-bucket", dest_prefix="wrong-prefix")
        errors, items, preview = app._load_bulk_upload_items(str(sheet))
        self.assertEqual(errors, [])
        self.assertEqual(items[0].destination_uri, "s3://chosen-bucket/e25/art.jpg")
        self.assertEqual(preview.first_destination_uri, items[0].destination_uri)

    def test_overwrite_declined_uploads_nothing_and_reports_cancelled(self):
        app = S3CopyApp.__new__(S3CopyApp)
        app.config = AppConfig()
        app.root = None
        app._get_active_credentials = Mock(return_value=None)
        app._enqueue_ui = Mock()
        app._call_on_ui_thread = Mock(return_value=False)
        app._bulk_upload_report_path = Mock(return_value=self.root / "report.csv")
        items = [DirectUploadItem(str(index), str(self.add_file(f"{index}.jpg")),
                                  S3ObjectRef("bucket", f"e01/{index}.jpg"),
                                  f"s3://bucket/e01/{index}.jpg") for index in (1, 2)]
        with patch("s3_copy_desktop_app.app.create_s3_client"), \
                patch("s3_copy_desktop_app.app.object_exists", side_effect=[True, False]) as exists, \
                patch("s3_copy_desktop_app.app.upload_local_file") as upload:
            app._bulk_upload_worker(items)
            self.assertEqual(exists.call_count, 2)
            upload.assert_not_called()
        with (self.root / "report.csv").open(newline="") as handle:
            rows = list(csv.DictReader(handle))
        self.assertEqual([row["status"] for row in rows], ["cancelled", "cancelled"])

    def test_legacy_csv_keeps_settings_destination(self):
        source = self.add_file("art.jpg")
        sheet = self.root / "legacy.csv"
        with sheet.open("w", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(["Local File Path", "Desired Move Folder"])
            writer.writerow([str(source), "e25"])
        app = S3CopyApp.__new__(S3CopyApp)
        app.config = AppConfig(dest_bucket="settings-bucket", dest_prefix="series/title/s01")
        errors, items, _ = app._load_bulk_upload_items(str(sheet))
        self.assertEqual(errors, [])
        self.assertEqual(items[0].destination_uri, "s3://settings-bucket/series/title/s01/e25/art.jpg")


if __name__ == "__main__":
    unittest.main()
