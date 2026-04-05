# huf0

Python bindings for [Huff0](https://github.com/Cyan4973/FiniteStateEntropy) —
a high-speed Huffman codec by Yann Collet (author of zstd).

## Install
```bash
git clone --recurse-submodules https://github.com/EIzharikov/huff0
cd huf0
pip install -e .
```

If you already cloned without submodules:
```bash
git submodule update --init --recursive
pip install -e .
```

## Usage
```python
import huf0

data = b"hello world " * 10000

compressed   = huf0.compress(data)
decompressed = huf0.decompress(compressed)
assert decompressed == data

ratio = len(compressed) / len(data)
print(f"ratio: {ratio:.4f}  savings: {(1-ratio)*100:.1f}%")
```

File helpers:
```python
stats = huf0.compress_file("model.safetensors", "model.huf")
# {'original_size': ..., 'compressed_size': ..., 'ratio': ..., 'savings_pct': ...}

huf0.decompress_file("model.huf", "model_restored.safetensors")
```

## Notes

- Chunk size: 128 KB (`HUF_BLOCKSIZE_MAX`)
- Incompressible chunks are stored raw automatically
- RLE (single repeated byte) is a special case
- No byte grouping — pure Huffman on raw bytes

## File format
```
[8]  magic "HUF0BLK\x00"
[8]  original_size   uint64 LE
[4]  chunk_size      uint32 LE
per chunk:
  [1]  type   0=raw  1=huffman  2=rle
  [4]  size   uint32 LE
  [N]  data
```