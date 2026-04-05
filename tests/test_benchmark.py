import os
import time

import huf0


def benchmark(name: str, data: bytes) -> None:
    size_mb = len(data) / 1024 / 1024

    t0 = time.perf_counter()
    compressed = huf0.compress(data)
    t1 = time.perf_counter()
    decomp = huf0.decompress(compressed)
    t2 = time.perf_counter()

    assert decomp == data

    comp_ms = (t1 - t0) * 1000
    decomp_ms = (t2 - t1) * 1000
    ratio = len(compressed) / len(data)

    print(f"\n{name}")
    print(f"  size        : {size_mb:.1f} MB")
    print(f"  ratio       : {ratio:.4f}  (savings {(1-ratio)*100:.1f}%)")
    print(f"  compress    : {comp_ms:.1f} ms  ({size_mb / (comp_ms/1000):.0f} MB/s)")
    print(
        f"  decompress  : {decomp_ms:.1f} ms  ({size_mb / (decomp_ms/1000):.0f} MB/s)"
    )


def test_benchmark_repetitive():
    benchmark("repetitive (b'hello ' * 500000)", b"hello " * 500_000)


def test_benchmark_bf16_exponents():
    import random

    random.seed(42)
    # скошенное распределение как у экспонент BF16
    data = bytes(
        (
            0x7F
            if random.random() < 0.70
            else 0x7E if random.random() < 0.67 else random.randint(0, 255)
        )
        for _ in range(128 * 1024)
    )
    benchmark("BF16 exponents (128 KB)", data)


def test_benchmark_random():
    data = os.urandom(128 * 1024)
    benchmark("random bytes (128 KB)", data)


def test_benchmark_1mb_repetitive():
    benchmark("repetitive 1 MB", b"\x7f\x00" * (512 * 1024))
