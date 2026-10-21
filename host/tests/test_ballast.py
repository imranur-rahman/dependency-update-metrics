"""The memory ballast (ticket #16, ADR 0001).

The ballast holds only the gap between the memory that the rest of the VM uses and 25% of
the RAM. It reads the memory use of the whole VM from /proc/meminfo, so the Job child
processes of the workers count as worker memory with no per-process bookkeeping.
"""

from dmhost.ballast import Ballast, MemoryInfo, ballast_bytes

GB = 1024**3


def test_quiet_host_gets_ballast_up_to_a_quarter_of_ram():
    # 24 GB VM, 1 GB in use, no ballast yet: the ballast fills 6 GB - 1 GB.
    memory = MemoryInfo(total_bytes=24 * GB, available_bytes=23 * GB)

    assert ballast_bytes(memory, held_bytes=0) == 5 * GB


def test_ballast_that_is_held_does_not_count_as_other_use():
    # The ballast holds 5 GB of the 6 GB in use, so the rest of the VM uses 1 GB: keep 5 GB.
    memory = MemoryInfo(total_bytes=24 * GB, available_bytes=18 * GB)

    assert ballast_bytes(memory, held_bytes=5 * GB) == 5 * GB


def test_ballast_releases_memory_when_workers_use_more():
    # The workers grew from 1 GB to 4 GB while the ballast held 5 GB: keep only 2 GB.
    memory = MemoryInfo(total_bytes=24 * GB, available_bytes=15 * GB)

    assert ballast_bytes(memory, held_bytes=5 * GB) == 2 * GB


def test_ballast_never_goes_below_zero_when_workers_use_more_than_the_target():
    memory = MemoryInfo(total_bytes=24 * GB, available_bytes=10 * GB)

    assert ballast_bytes(memory, held_bytes=0) == 0
    assert ballast_bytes(memory, held_bytes=2 * GB) == 0


MB = 1024**2


def _meminfo(total_mb: int, available_mb: int) -> str:
    # The format of /proc/meminfo, in kB, with the other lines that the file also has.
    return (
        f"MemTotal:       {total_mb * 1024} kB\n"
        f"MemFree:        {available_mb * 512} kB\n"
        f"MemAvailable:   {available_mb * 1024} kB\n"
        "Buffers:           2048 kB\n"
        "SwapTotal:            0 kB\n"
    )


def test_ballast_reads_meminfo_and_holds_then_releases_the_gap():
    # A 64 MB "VM": the target is 16 MB. The reader returns the next text at each call.
    texts = iter(
        [
            _meminfo(total_mb=64, available_mb=60),  # 4 MB in use: hold 12 MB
            _meminfo(total_mb=64, available_mb=48),  # 4 MB + the 12 MB ballast: keep 12 MB
            _meminfo(total_mb=64, available_mb=42),  # workers grew to 10 MB: keep 6 MB
            _meminfo(total_mb=64, available_mb=20),  # workers use 38 MB: hold nothing
        ]
    )
    ballast = Ballast(read_meminfo=lambda: next(texts), chunk_bytes=1 * MB)

    held = [ballast.adjust() for _ in range(4)]

    assert held == [12 * MB, 12 * MB, 6 * MB, 0]
    assert ballast.held_bytes == 0


def test_ballast_holds_whole_chunks_only():
    # The gap is 16 - 4.5 = 11.5 MB; the ballast rounds down to whole 4 MB chunks.
    text = f"MemTotal:       {64 * 1024} kB\n" f"MemAvailable:   {int(60 * 1024 - 0.5 * 1024)} kB\n"
    ballast = Ballast(read_meminfo=lambda: text, chunk_bytes=4 * MB)

    assert ballast.adjust() == 8 * MB
