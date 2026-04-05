#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <stdint.h>
#include <string.h>

#define HUF_STATIC_LINKING_ONLY
#include "huf.h"

/* ─────────────────────────────────────────────────────────────────────────
 * compress(data: bytes | bytearray | memoryview) -> bytes
 *
 * Сжимает данные чанками по 128KB (HUF_BLOCKSIZE_MAX).
 *
 * Формат вывода:
 *   [8]  magic "HUF0BLK\x00"
 *   [8]  original_size  uint64_t LE
 *   [4]  chunk_size     uint32_t LE
 *   затем для каждого чанка:
 *     [1]  type  0=raw, 1=huffman, 2=rle
 *     [4]  size  uint32_t LE  — размер следующих данных
 *     [N]  data
 * ───────────────────────────────────────────────────────────────────────── */

#define MAGIC          "HUF0BLK\x00"
#define MAGIC_LEN      8
#define CHUNK_SIZE     (128 * 1024)
#define TYPE_RAW       0
#define TYPE_HUFFMAN   1
#define TYPE_RLE       2

static void write_u32(uint8_t *buf, size_t offset, uint32_t v) {
    memcpy(buf + offset, &v, 4);
}
static void write_u64(uint8_t *buf, size_t offset, uint64_t v) {
    memcpy(buf + offset, &v, 8);
}
static uint32_t read_u32(const uint8_t *buf, size_t offset) {
    uint32_t v; memcpy(&v, buf + offset, 4); return v;
}
static uint64_t read_u64(const uint8_t *buf, size_t offset) {
    uint64_t v; memcpy(&v, buf + offset, 8); return v;
}

/* ── compress ────────────────────────────────────────────────────────────── */

static PyObject *py_compress(PyObject *self, PyObject *args) {
    Py_buffer view;

    if (!PyArg_ParseTuple(args, "y*", &view)) {
        return NULL;
    }

    const uint8_t *src      = (const uint8_t *)view.buf;
    size_t         src_size = (size_t)view.len;

    /* верхняя оценка размера выходного буфера:
       заголовок + для каждого чанка (1+4) метаданные + worst-case данные */
    size_t n_chunks  = (src_size + CHUNK_SIZE - 1) / CHUNK_SIZE;
    if (n_chunks == 0) n_chunks = 1;
    size_t max_out   = MAGIC_LEN + 8 + 4
                       + n_chunks * (1 + 4 + HUF_COMPRESSBOUND(CHUNK_SIZE));

    uint8_t *out = (uint8_t *)malloc(max_out);
    if (!out) {
        PyBuffer_Release(&view);
        return PyErr_NoMemory();
    }

    uint8_t *huf_buf = (uint8_t *)malloc(HUF_COMPRESSBOUND(CHUNK_SIZE));
    if (!huf_buf) {
        free(out);
        PyBuffer_Release(&view);
        return PyErr_NoMemory();
    }

    /* заголовок */
    memcpy(out, MAGIC, MAGIC_LEN);
    write_u64(out, MAGIC_LEN,         (uint64_t)src_size);
    write_u32(out, MAGIC_LEN + 8,     (uint32_t)CHUNK_SIZE);
    size_t pos = MAGIC_LEN + 8 + 4;

    size_t offset = 0;
    while (offset < src_size) {
        size_t chunk_len = src_size - offset;
        if (chunk_len > CHUNK_SIZE) chunk_len = CHUNK_SIZE;

        size_t csize = HUF_compress(huf_buf,
                                    HUF_COMPRESSBOUND(CHUNK_SIZE),
                                    src + offset, chunk_len);

        if (HUF_isError(csize) || csize == 0) {
            /* несжимаемо — raw */
            out[pos++] = TYPE_RAW;
            write_u32(out, pos, (uint32_t)chunk_len); pos += 4;
            memcpy(out + pos, src + offset, chunk_len);
            pos += chunk_len;
        } else if (csize == 1) {
            /* RLE */
            out[pos++] = TYPE_RLE;
            write_u32(out, pos, 1); pos += 4;
            out[pos++] = huf_buf[0];
        } else {
            /* Huffman */
            out[pos++] = TYPE_HUFFMAN;
            write_u32(out, pos, (uint32_t)csize); pos += 4;
            memcpy(out + pos, huf_buf, csize);
            pos += csize;
        }

        offset += chunk_len;
    }

    free(huf_buf);
    PyBuffer_Release(&view);

    PyObject *result = PyBytes_FromStringAndSize((const char *)out, pos);
    free(out);
    return result;
}

/* ── decompress ─────────────────────────────────────────────────────────── */

static PyObject *py_decompress(PyObject *self, PyObject *args) {
    Py_buffer view;

    if (!PyArg_ParseTuple(args, "y*", &view)) {
        return NULL;
    }

    const uint8_t *src      = (const uint8_t *)view.buf;
    size_t         src_size = (size_t)view.len;

    /* проверяем magic */
    if (src_size < MAGIC_LEN + 8 + 4 ||
        memcmp(src, MAGIC, MAGIC_LEN) != 0) {
        PyBuffer_Release(&view);
        PyErr_SetString(PyExc_ValueError,
                        "Invalid data: missing HUF0BLK magic header");
        return NULL;
    }

    uint64_t original_size = read_u64(src, MAGIC_LEN);
    uint32_t chunk_size    = read_u32(src, MAGIC_LEN + 8);

    /* аллоцируем выходной буфер */
    uint8_t *out = (uint8_t *)malloc(original_size);
    if (!out) {
        PyBuffer_Release(&view);
        return PyErr_NoMemory();
    }

    uint8_t *tmp = (uint8_t *)malloc(chunk_size);
    if (!tmp) {
        free(out);
        PyBuffer_Release(&view);
        return PyErr_NoMemory();
    }

    size_t   in_pos  = MAGIC_LEN + 8 + 4;
    size_t   out_pos = 0;

    while (out_pos < (size_t)original_size) {
        if (in_pos + 5 > src_size) {
            PyErr_SetString(PyExc_ValueError, "Truncated compressed data");
            goto error;
        }

        uint8_t  type  = src[in_pos++];
        uint32_t csize = read_u32(src, in_pos); in_pos += 4;

        if (in_pos + csize > src_size) {
            PyErr_SetString(PyExc_ValueError, "Chunk data out of bounds");
            goto error;
        }

        size_t remain    = (size_t)original_size - out_pos;
        size_t dst_avail = remain < chunk_size ? remain : chunk_size;

        if (type == TYPE_RAW) {
            memcpy(out + out_pos, src + in_pos, csize);
            out_pos += csize;

        } else if (type == TYPE_HUFFMAN) {
            size_t dsize = HUF_decompress(tmp, dst_avail,
                                          src + in_pos, csize);
            if (HUF_isError(dsize)) {
                PyErr_Format(PyExc_RuntimeError,
                             "HUF_decompress error: %s",
                             HUF_getErrorName(dsize));
                goto error;
            }
            memcpy(out + out_pos, tmp, dsize);
            out_pos += dsize;

        } else if (type == TYPE_RLE) {
            /* передаём 1 байт в HUF_decompress — он развернёт RLE */
            size_t dsize = HUF_decompress(tmp, dst_avail,
                                          src + in_pos, 1);
            if (HUF_isError(dsize)) {
                PyErr_Format(PyExc_RuntimeError,
                             "HUF_decompress RLE error: %s",
                             HUF_getErrorName(dsize));
                goto error;
            }
            memcpy(out + out_pos, tmp, dsize);
            out_pos += dsize;

        } else {
            PyErr_Format(PyExc_ValueError,
                         "Unknown chunk type: %d", type);
            goto error;
        }

        in_pos += csize;
    }

    free(tmp);
    PyBuffer_Release(&view);

    PyObject *result = PyBytes_FromStringAndSize((const char *)out,
                                                 (Py_ssize_t)original_size);
    free(out);
    return result;

error:
    free(tmp);
    free(out);
    PyBuffer_Release(&view);
    return NULL;
}

/* ── compress_bound ─────────────────────────────────────────────────────── */

static PyObject *py_compress_bound(PyObject *self, PyObject *args) {
    Py_ssize_t size;
    if (!PyArg_ParseTuple(args, "n", &size)) return NULL;
    size_t n_chunks = ((size_t)size + CHUNK_SIZE - 1) / CHUNK_SIZE;
    if (n_chunks == 0) n_chunks = 1;
    size_t bound = MAGIC_LEN + 8 + 4
                   + n_chunks * (1 + 4 + HUF_COMPRESSBOUND(CHUNK_SIZE));
    return PyLong_FromSize_t(bound);
}

/* ── module definition ───────────────────────────────────────────────────── */

static PyMethodDef Huf0Methods[] = {
    {"compress",       py_compress,       METH_VARARGS,
     "compress(data) -> bytes\n\n"
     "Compress bytes using Huff0 (128 KB chunks).\n"
     "Accepts bytes, bytearray, or memoryview."},

    {"decompress",     py_decompress,     METH_VARARGS,
     "decompress(data) -> bytes\n\n"
     "Decompress data compressed with huf0.compress()."},

    {"compress_bound", py_compress_bound, METH_VARARGS,
     "compress_bound(size) -> int\n\n"
     "Return worst-case compressed size for input of given length."},

    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef huf0module = {
    PyModuleDef_HEAD_INIT,
    "_huf0_core",
    "Huff0 lossless compression (Huf0 from FiniteStateEntropy)",
    -1,
    Huf0Methods
};

PyMODINIT_FUNC PyInit__huf0_core(void) {
    return PyModule_Create(&huf0module);
}
