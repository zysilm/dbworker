import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from zipfile import ZipFile

import httpx

from imagededup_benckmark.dataset import RemoteArchive, download_dataset


class DatasetTest(unittest.TestCase):
    def archive_client(self, content: bytes, *, ignore_range: bool = False) -> httpx.Client:
        def respond(request: httpx.Request) -> httpx.Response:
            if request.method == "HEAD":
                return httpx.Response(200, headers={"Content-Length": str(len(content)), "ETag": '"test"'})
            self.assertEqual(request.headers["If-Range"], '"test"')
            if ignore_range:
                return httpx.Response(200, content=content)
            start, end = map(int, request.headers["Range"].removeprefix("bytes=").split("-"))
            return httpx.Response(206, content=content[start:end + 1],
                                  headers={"Content-Range": f"bytes {start}-{end}/{len(content)}"})
        return httpx.Client(transport=httpx.MockTransport(respond))

    def test_seek_read_across_blocks_and_eof(self) -> None:
        content = bytes(range(256)) * 5
        with self.archive_client(content) as client, RemoteArchive(client, "https://test/archive") as archive:
            archive.block_size = 100
            archive.seek(99)
            self.assertEqual(archive.read(303), content[99:402])
            archive.seek(-5, 2)
            self.assertEqual(archive.read(), content[-5:])
            self.assertEqual(archive.read(1), b"")

    def test_range_ignored_fails_without_downloading_full_archive(self) -> None:
        with self.archive_client(b"archive", ignore_range=True) as client:
            archive = RemoteArchive(client, "https://test/archive")
            with self.assertRaisesRegex(OSError, "byte ranges"):
                archive.read(1)

    def test_selects_numbered_images_and_reuses_completed_files(self) -> None:
        buffer = io.BytesIO()
        with ZipFile(buffer, "w") as archive:
            for number in range(25000, 0, -1):
                archive.writestr(f"mirflickr/im{number}.jpg", f"image-{number}")
            archive.writestr("../escape.jpg", "must not extract")
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory, "images")
            for attempt in range(2):
                with patch("imagededup_benckmark.dataset.httpx.Client",
                           return_value=self.archive_client(buffer.getvalue())):
                    paths = download_dataset(target, limit=2)
                self.assertEqual([path.name for path in paths], ["im1.jpg", "im2.jpg"])
                self.assertEqual(paths[0].read_bytes(), b"image-1")
                self.assertEqual(sorted(path.name for path in target.iterdir()), ["im1.jpg", "im2.jpg"])
                if attempt == 0:
                    paths[0].write_bytes(b"corrupt")  # Same size: checksum must catch it.
            self.assertFalse(Path(directory, "escape.jpg").exists())

    def test_invalid_limit(self) -> None:
        with self.assertRaises(ValueError):
            download_dataset(limit=0)
