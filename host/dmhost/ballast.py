"""The memory ballast of the Host (ticket #16, ADR 0001).

Oracle can reclaim an Always Free A1 instance when its memory use stays below 20% at the
95th percentile over 7 days. The ballast holds anonymous memory, so that the memory use of
the VM stays at or above a target of 25% of the RAM when the Host is quiet.

The ballast holds only the gap between the target and the memory that the rest of the VM
uses. It reads that use for the whole VM from ``/proc/meminfo`` (a container sees the
values of the VM), so the workers and their Job child processes all count, with no
per-process bookkeeping. When the workers use more, the ballast releases memory.
"""

from __future__ import annotations

import logging
import mmap
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, List

logger = logging.getLogger("dmhost.ballast")

TARGET_FRACTION = 0.25
# The ballast holds and releases memory in chunks of this size.
CHUNK_BYTES = 64 * 1024**2
# The ballast checks the memory use this often. A short interval releases memory soon
# after the workers need it; the kernel stops the ballast first (oom_score_adj 1000)
# when the workers grow faster than that.
INTERVAL_SECONDS = 2.0
MEMINFO = Path("/proc/meminfo")


@dataclass(frozen=True)
class MemoryInfo:
    """The memory of the VM, from ``MemTotal`` and ``MemAvailable`` in /proc/meminfo."""

    total_bytes: int
    available_bytes: int


def ballast_bytes(
    memory: MemoryInfo, held_bytes: int, target_fraction: float = TARGET_FRACTION
) -> int:
    """Return the number of bytes that the ballast must hold.

    ``held_bytes`` is what the ballast holds now. It is part of the used memory in
    ``memory``, so the function removes it to get the use of the rest of the VM.
    """
    target = int(memory.total_bytes * target_fraction)
    other = memory.total_bytes - memory.available_bytes - held_bytes
    return max(0, target - other)


def parse_meminfo(text: str) -> MemoryInfo:
    """Read ``MemTotal`` and ``MemAvailable`` (in kB) from the text of /proc/meminfo."""
    values = {}
    for line in text.splitlines():
        name, _, rest = line.partition(":")
        fields = rest.split()
        if fields:
            values[name.strip()] = int(fields[0]) * 1024
    return MemoryInfo(total_bytes=values["MemTotal"], available_bytes=values["MemAvailable"])


class Ballast:
    """Holds anonymous memory and changes its size to the gap of ``ballast_bytes``.

    Each chunk is a private anonymous mapping, and the ballast writes to every page of
    it. So the kernel gives each page real memory, and the memory counts as used (not
    as cache) in /proc/meminfo and in the OCI memory metric.
    """

    def __init__(
        self,
        read_meminfo: Callable[[], str] = MEMINFO.read_text,
        chunk_bytes: int = CHUNK_BYTES,
        target_fraction: float = TARGET_FRACTION,
    ) -> None:
        self._read_meminfo = read_meminfo
        self._chunk_bytes = chunk_bytes
        self._target_fraction = target_fraction
        self._chunks: List[mmap.mmap] = []

    @property
    def held_bytes(self) -> int:
        return len(self._chunks) * self._chunk_bytes

    def adjust(self) -> int:
        """Read the memory use once, hold or release chunks, and return the held bytes."""
        memory = parse_meminfo(self._read_meminfo())
        wanted = ballast_bytes(memory, self.held_bytes, self._target_fraction)
        chunks = wanted // self._chunk_bytes
        while len(self._chunks) > chunks:
            self._chunks.pop().close()
        while len(self._chunks) < chunks:
            self._chunks.append(_touched_chunk(self._chunk_bytes))
        return self.held_bytes

    def run_forever(self, interval_seconds: float = INTERVAL_SECONDS) -> None:
        last = -1
        while True:
            held = self.adjust()
            if held != last:
                logger.info("The ballast holds %d MB", held // 1024**2)
                last = held
            time.sleep(interval_seconds)


def _touched_chunk(size: int) -> mmap.mmap:
    chunk = mmap.mmap(-1, size, flags=mmap.MAP_PRIVATE | mmap.MAP_ANONYMOUS)
    # One write to each page makes the kernel give the page real memory.
    for offset in range(0, size, mmap.PAGESIZE):
        chunk[offset] = 1
    return chunk


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    Ballast().run_forever()


if __name__ == "__main__":
    main()
