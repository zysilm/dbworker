"""Download real MIRFLICKR images without storing the 3 GB ZIP archive."""

import argparse
import io
import os
import re
import shutil
import zlib
from pathlib import Path
from zipfile import ZipFile

import httpx

DATASET_URL = "https://press.liacs.nl/mirflickr/mirflickr25k.v3b/mirflickr25k.zip"
DEFAULT_DIRECTORY = Path(__file__).resolve().parents[2] / "data" / "mirflickr25k"


class RemoteArchive(io.RawIOBase):
    """Seekable HTTP range reader, with one bounded block cached in memory."""

    def __init__(self, client: httpx.Client, url: str) -> None:
        super().__init__()
        self.client = client
        self.url = url
        response = client.head(url)
        response.raise_for_status()
        self.size = int(response.headers["content-length"])
        self.etag = response.headers.get("etag")
        self.position = 0
        self.block_size = 4 * 1024 * 1024
        self.block_start = -1
        self.block = b""

    def seekable(self) -> bool:
        return True

    def readable(self) -> bool:
        return True

    def tell(self) -> int:
        return self.position

    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        position = offset + {os.SEEK_SET: 0, os.SEEK_CUR: self.position, os.SEEK_END: self.size}[whence]
        if position < 0:
            raise ValueError("Negative archive position")
        self.position = position
        return position

    def read(self, size: int = -1) -> bytes:
        remaining = max(0, self.size - self.position)
        wanted = remaining if size < 0 else min(size, remaining)
        parts: list[bytes] = []
        while wanted:
            start = self.position // self.block_size * self.block_size
            if self.block_start != start:
                end = min(start + self.block_size, self.size) - 1
                headers = {"Range": f"bytes={start}-{end}"}
                if self.etag:
                    headers["If-Range"] = self.etag
                with self.client.stream("GET", self.url, headers=headers) as response:
                    if response.status_code != 206 or response.headers.get("content-range") != f"bytes {start}-{end}/{self.size}":
                        raise OSError("Dataset server did not honor byte ranges or the archive changed; rerun the download")
                    block = response.read()
                if len(block) != end - start + 1:
                    raise OSError("Incomplete dataset download")
                self.block_start, self.block = start, block
            offset = self.position - self.block_start
            count = min(wanted, len(self.block) - offset)
            parts.append(self.block[offset:offset + count])
            self.position += count
            wanted -= count
        return b"".join(parts)


def download_dataset(directory: Path = DEFAULT_DIRECTORY, limit: int = 25000) -> list[Path]:
    """Extract the first N numbered images; ZIP CRC checks validate downloaded files.

    Completed images are reused on subsequent runs. Partial files are replaced
    only after successful extraction. No image content is synthesized.
    """
    if not 1 <= limit <= 25000:
        raise ValueError("limit must be between 1 and 25000")
    directory = directory.expanduser().resolve()
    directory.mkdir(parents=True, exist_ok=True)
    with httpx.Client(timeout=60, follow_redirects=True) as client:
        with RemoteArchive(client, DATASET_URL) as remote, ZipFile(remote) as archive:
            images = [entry for entry in archive.infolist()
                      if re.fullmatch(r"mirflickr/im\d+\.jpg", entry.filename)]
            images.sort(key=lambda entry: int(Path(entry.filename).stem[2:]))
            if len(images) != 25000:
                raise ValueError(f"Expected 25000 MIRFLICKR images, found {len(images)}")
            selected = images[:limit]
            # Read in archive order to reuse adjacent HTTP blocks.
            for index, entry in enumerate(sorted(selected, key=lambda entry: entry.header_offset), 1):
                target = directory / Path(entry.filename).name
                checksum = 0
                if target.exists() and target.stat().st_size == entry.file_size:
                    with target.open("rb") as cached:
                        while chunk := cached.read(1024 * 1024):
                            checksum = zlib.crc32(chunk, checksum)
                if not target.exists() or target.stat().st_size != entry.file_size or checksum != entry.CRC:
                    partial = target.with_suffix(".jpg.part")
                    try:
                        with archive.open(entry) as source, partial.open("wb") as output:
                            shutil.copyfileobj(source, output)
                        partial.replace(target)
                    finally:
                        partial.unlink(missing_ok=True)
                if index % 100 == 0 or index == limit:
                    print(f"MIRFLICKR: {index}/{limit} images available in {directory}", flush=True)
    return [directory / Path(entry.filename).name for entry in selected]


def run() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, default=DEFAULT_DIRECTORY)
    parser.add_argument("--limit", type=int, default=25000, help="Number of real images to download (1–25000)")
    args = parser.parse_args()
    download_dataset(args.directory, args.limit)


if __name__ == "__main__":
    run()
