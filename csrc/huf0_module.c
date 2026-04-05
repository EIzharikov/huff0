#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <stdint.h>
#include <string.h>

/* ── cross-platform threads ─────────────────────────────────────────────── */
#ifdef _WIN32
#  include <windows.h>
#  define THREAD_T         HANDLE
#  define MUTEX_T          CRITICAL_SECTION
#  define MUTEX_INIT(m)    InitializeCriticalSection(&(m))
#  define MUTEX_DESTROY(m) DeleteCriticalSection(&(m))
#  define MUTEX_LOCK(m)    EnterCriticalSection(&(m))
#  define MUTEX_UNLOCK(m)  LeaveCriticalSection(&(m))
#  define THREAD_RET       DWORD WINAPI
#  define THREAD_ARG       LPVOID
typedef DWORD (WINAPI *win_thread_fn)(LPVOID);
static int thread_create(HANDLE *t, win_thread_fn fn, void *arg) {
    *t = CreateThread(NULL, 0, fn, arg, 0, NULL);
    return (*t == NULL) ? -1 : 0;
}
static void thread_join(HANDLE t) {
    WaitForSingleObject(t, INFINITE);
    CloseHandle(t);
}
static int get_cpu_count(void) {
    SYSTEM_INFO si;
    GetSystemInfo(&si);
    return (int)si.dwNumberOfProcessors;
}
#else
#  include <pthread.h>
#  include <unistd.h>
#  define THREAD_T         pthread_t
#  define MUTEX_T          pthread_mutex_t
#  define MUTEX_INIT(m)    pthread_mutex_init(&(m), NULL)
#  define MUTEX_DESTROY(m) pthread_mutex_destroy(&(m))
#  define MUTEX_LOCK(m)    pthread_mutex_lock(&(m))
#  define MUTEX_UNLOCK(m)  pthread_mutex_unlock(&(m))
#  define THREAD_RET       void *
#  define THREAD_ARG       void *
static int thread_create(THREAD_T *t, void *(*fn)(void *), void *arg) {
    return pthread_create(t, NULL, fn, arg);
}
static void thread_join(THREAD_T t) { pthread_join(t, NULL); }
static int get_cpu_count(void) {
    int n = (int)sysconf(_SC_NPROCESSORS_ONLN);
    return n > 0 ? n : 1;
}
#endif

#define HUF_STATIC_LINKING_ONLY
#include "huf.h"

#define MAGIC        "HUF0BLK\x00"
#define MAGIC_LEN    8
#define CHUNK_SIZE   (128 * 1024)  /* HUF_BLOCKSIZE_MAX */
#define TYPE_RAW     0
#define TYPE_HUFFMAN 1
#define TYPE_RLE     2
#define MAX_THREADS  16

/* ── helpers ────────────────────────────────────────────────────────────── */

static void write_u32(uint8_t *buf, size_t off, uint32_t v) {
    memcpy(buf + off, &v, 4);
}
static void write_u64(uint8_t *buf, size_t off, uint64_t v) {
    memcpy(buf + off, &v, 8);
}
static uint32_t read_u32(const uint8_t *buf, size_t off) {
    uint32_t v; memcpy(&v, buf + off, 4); return v;
}
static uint64_t read_u64(const uint8_t *buf, size_t off) {
    uint64_t v; memcpy(&v, buf + off, 8); return v;
}

/* ── compress ────────────────────────────────────────────────────────────── */

/* Result of compressing a single chunk.
 * Each worker thread writes into its own slot — no data races. */
typedef struct {
    uint8_t *data;   /* pointer to compressed bytes */
    uint32_t size;   /* size of compressed data */
    uint8_t  type;   /* TYPE_RAW / TYPE_HUFFMAN / TYPE_RLE */
    int      owned;  /* 1 = data must be free()'d, 0 = pointer into src */
} ChunkResult;

/* Shared context passed to every compression worker thread. */
typedef struct {
    const uint8_t *src;
    size_t         src_size;
    size_t         n_chunks;
    ChunkResult   *results;
    size_t         next_chunk;  /* work-stealing counter */
    MUTEX_T        mutex;
} CompressCtx;

static THREAD_RET compress_worker(THREAD_ARG arg) {
    CompressCtx *ctx = (CompressCtx *)arg;

    while (1) {
        /* grab next available chunk */
        MUTEX_LOCK(ctx->mutex);
        size_t idx = ctx->next_chunk++;
        MUTEX_UNLOCK(ctx->mutex);

        if (idx >= ctx->n_chunks) break;

        size_t         offset    = idx * CHUNK_SIZE;
        size_t         chunk_len = ctx->src_size - offset;
        if (chunk_len > CHUNK_SIZE) chunk_len = CHUNK_SIZE;

        const uint8_t *csrc  = ctx->src + offset;
        ChunkResult   *res   = &ctx->results[idx];
        size_t         bound = HUF_COMPRESSBOUND(chunk_len);
        uint8_t       *hbuf  = (uint8_t *)malloc(bound);

        if (!hbuf) {
            res->data = NULL; res->size = 0;
            res->type = TYPE_RAW; res->owned = 0;
#ifdef _WIN32
            return 1;
#else
            pthread_exit((void *)-1);
#endif
        }

        size_t csize = HUF_compress(hbuf, bound, csrc, chunk_len);

        if (HUF_isError(csize) || csize == 0) {
            /* incompressible — store raw, reuse pointer into src */
            free(hbuf);
            res->data  = (uint8_t *)csrc;
            res->size  = (uint32_t)chunk_len;
            res->type  = TYPE_RAW;
            res->owned = 0;
        } else if (csize == 1) {
            /* RLE: single repeated byte */
            res->data  = hbuf;
            res->size  = 1;
            res->type  = TYPE_RLE;
            res->owned = 1;
        } else {
            /* Huffman compressed */
            res->data  = hbuf;
            res->size  = (uint32_t)csize;
            res->type  = TYPE_HUFFMAN;
            res->owned = 1;
        }
    }
#ifdef _WIN32
    return 0;
#else
    pthread_exit(NULL);
#endif
}

static PyObject *py_compress(PyObject *self, PyObject *args, PyObject *kwargs) {
    Py_buffer view;
    int       threads = 0;
    static char *kwlist[] = {"data", "threads", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "y*|i", kwlist,
                                     &view, &threads))
        return NULL;

    const uint8_t *src      = (const uint8_t *)view.buf;
    size_t         src_size = (size_t)view.len;

    if (threads <= 0) threads = get_cpu_count();
    if (threads > MAX_THREADS) threads = MAX_THREADS;

    size_t n_chunks = (src_size + CHUNK_SIZE - 1) / CHUNK_SIZE;
    if (n_chunks == 0) n_chunks = 1;
    if ((size_t)threads > n_chunks) threads = (int)n_chunks;

    ChunkResult *results = (ChunkResult *)calloc(n_chunks, sizeof(ChunkResult));
    if (!results) { PyBuffer_Release(&view); return PyErr_NoMemory(); }

    CompressCtx ctx;
    ctx.src        = src;
    ctx.src_size   = src_size;
    ctx.n_chunks   = n_chunks;
    ctx.results    = results;
    ctx.next_chunk = 0;
    MUTEX_INIT(ctx.mutex);

    THREAD_T *tids = (THREAD_T *)malloc(threads * sizeof(THREAD_T));
    if (!tids) {
        MUTEX_DESTROY(ctx.mutex); free(results);
        PyBuffer_Release(&view); return PyErr_NoMemory();
    }

    for (int t = 0; t < threads; t++) {
        if (thread_create(&tids[t], compress_worker, &ctx) != 0) {
            for (int j = 0; j < t; j++) thread_join(tids[j]);
            free(tids); MUTEX_DESTROY(ctx.mutex); free(results);
            PyBuffer_Release(&view);
            PyErr_SetString(PyExc_RuntimeError, "thread_create failed");
            return NULL;
        }
    }
    for (int t = 0; t < threads; t++) thread_join(tids[t]);
    free(tids);
    MUTEX_DESTROY(ctx.mutex);

    /* assemble output buffer: header + per-chunk metadata + data */
    size_t max_out = MAGIC_LEN + 8 + 4
                   + n_chunks * (1 + 4 + HUF_COMPRESSBOUND(CHUNK_SIZE));
    uint8_t *out = (uint8_t *)malloc(max_out);
    if (!out) {
        for (size_t i = 0; i < n_chunks; i++)
            if (results[i].owned) free(results[i].data);
        free(results); PyBuffer_Release(&view);
        return PyErr_NoMemory();
    }

    memcpy(out, MAGIC, MAGIC_LEN);
    write_u64(out, MAGIC_LEN,     (uint64_t)src_size);
    write_u32(out, MAGIC_LEN + 8, (uint32_t)CHUNK_SIZE);
    size_t pos = MAGIC_LEN + 8 + 4;

    /* write chunks in order — guaranteed because each thread wrote
     * into its own results[idx] slot */
    for (size_t i = 0; i < n_chunks; i++) {
        ChunkResult *r = &results[i];
        out[pos++] = r->type;
        write_u32(out, pos, r->size); pos += 4;
        memcpy(out + pos, r->data, r->size); pos += r->size;
        if (r->owned) free(r->data);
    }

    free(results);
    PyBuffer_Release(&view);
    PyObject *result = PyBytes_FromStringAndSize((const char *)out, pos);
    free(out);
    return result;
}

/* ── decompress ─────────────────────────────────────────────────────────── */

/* Shared context for decompression workers.
 * Metadata arrays are filled in a single-threaded pass before workers
 * start, so each thread knows exactly where to read and write without
 * any coordination during the decompression phase. */
typedef struct {
    const uint8_t *src;
    size_t         n_chunks;
    uint32_t       chunk_size;
    uint64_t       original_size;
    uint8_t       *types;         /* compression type per chunk */
    size_t        *src_offsets;   /* byte offset of chunk data in src */
    uint32_t      *comp_sizes;    /* compressed size per chunk */
    size_t        *dst_offsets;   /* byte offset in output buffer */
    size_t        *decomp_sizes;  /* decompressed size per chunk */
    uint8_t       *out;           /* output buffer */
    size_t         next_chunk;    /* work-stealing counter */
    MUTEX_T        mutex;
    int            error;         /* set to 1 by any failing worker */
} DecompressCtx;

static THREAD_RET decompress_worker(THREAD_ARG arg) {
    DecompressCtx *ctx = (DecompressCtx *)arg;

    while (1) {
        MUTEX_LOCK(ctx->mutex);
        size_t idx = ctx->next_chunk++;
        MUTEX_UNLOCK(ctx->mutex);

        if (idx >= ctx->n_chunks) break;

        uint8_t        type      = ctx->types[idx];
        size_t         src_off   = ctx->src_offsets[idx];
        uint32_t       comp_size = ctx->comp_sizes[idx];
        size_t         dst_off   = ctx->dst_offsets[idx];
        size_t         decomp    = ctx->decomp_sizes[idx];
        const uint8_t *csrc      = ctx->src + src_off;
        uint8_t       *cdst      = ctx->out + dst_off;

        if (type == TYPE_RAW) {
            memcpy(cdst, csrc, comp_size);
        } else if (type == TYPE_HUFFMAN || type == TYPE_RLE) {
            /* RLE: Huff0 stored only 1 byte and expands it internally */
            size_t n = HUF_decompress(cdst, decomp, csrc,
                                      type == TYPE_RLE ? 1 : comp_size);
            if (HUF_isError(n)) {
                ctx->error = 1;
#ifdef _WIN32
                return 1;
#else
                pthread_exit((void *)-1);
#endif
            }
        } else {
            ctx->error = 1;
#ifdef _WIN32
            return 1;
#else
            pthread_exit((void *)-1);
#endif
        }
    }
#ifdef _WIN32
    return 0;
#else
    pthread_exit(NULL);
#endif
}

static PyObject *py_decompress(PyObject *self, PyObject *args, PyObject *kwargs) {
    Py_buffer view;
    int       threads = 0;
    static char *kwlist[] = {"data", "threads", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "y*|i", kwlist,
                                     &view, &threads))
        return NULL;

    const uint8_t *src      = (const uint8_t *)view.buf;
    size_t         src_size = (size_t)view.len;

    if (src_size < MAGIC_LEN + 8 + 4 ||
        memcmp(src, MAGIC, MAGIC_LEN) != 0) {
        PyBuffer_Release(&view);
        PyErr_SetString(PyExc_ValueError,
                        "Invalid data: missing HUF0BLK magic header");
        return NULL;
    }

    uint64_t original_size = read_u64(src, MAGIC_LEN);
    uint32_t chunk_size    = read_u32(src, MAGIC_LEN + 8);
    size_t   n_chunks      = (original_size + chunk_size - 1) / chunk_size;
    if (n_chunks == 0) n_chunks = 1;

    if (threads <= 0) threads = get_cpu_count();
    if (threads > MAX_THREADS) threads = MAX_THREADS;
    if ((size_t)threads > n_chunks) threads = (int)n_chunks;

    uint8_t  *types        = (uint8_t  *)malloc(n_chunks);
    size_t   *src_offsets  = (size_t   *)malloc(n_chunks * sizeof(size_t));
    uint32_t *comp_sizes   = (uint32_t *)malloc(n_chunks * sizeof(uint32_t));
    size_t   *dst_offsets  = (size_t   *)malloc(n_chunks * sizeof(size_t));
    size_t   *decomp_sizes = (size_t   *)malloc(n_chunks * sizeof(size_t));

    if (!types || !src_offsets || !comp_sizes ||
        !dst_offsets || !decomp_sizes) {
        free(types); free(src_offsets); free(comp_sizes);
        free(dst_offsets); free(decomp_sizes);
        PyBuffer_Release(&view);
        return PyErr_NoMemory();
    }

    /* single-threaded metadata pass: parse all chunk headers up front
     * so workers can read/write at known offsets without coordination */
    size_t in_pos  = MAGIC_LEN + 8 + 4;
    size_t dst_pos = 0;

    for (size_t i = 0; i < n_chunks; i++) {
        if (in_pos + 5 > src_size) {
            PyErr_SetString(PyExc_ValueError, "Truncated compressed data");
            goto meta_err;
        }
        uint8_t  type  = src[in_pos++];
        uint32_t csize = read_u32(src, in_pos); in_pos += 4;

        if (in_pos + csize > src_size) {
            PyErr_SetString(PyExc_ValueError, "Chunk data out of bounds");
            goto meta_err;
        }
        size_t remain = (size_t)original_size - dst_pos;
        size_t dsize  = remain < chunk_size ? remain : chunk_size;

        types[i]        = type;
        src_offsets[i]  = in_pos;
        comp_sizes[i]   = csize;
        dst_offsets[i]  = dst_pos;
        decomp_sizes[i] = dsize;

        in_pos  += (type == TYPE_RLE) ? 1 : csize;
        dst_pos += dsize;
    }

    uint8_t *out = (uint8_t *)malloc(original_size ? original_size : 1);
    if (!out) { PyErr_NoMemory(); goto meta_err; }

    {
        DecompressCtx ctx;
        ctx.src           = src;
        ctx.n_chunks      = n_chunks;
        ctx.chunk_size    = chunk_size;
        ctx.original_size = original_size;
        ctx.types         = types;
        ctx.src_offsets   = src_offsets;
        ctx.comp_sizes    = comp_sizes;
        ctx.dst_offsets   = dst_offsets;
        ctx.decomp_sizes  = decomp_sizes;
        ctx.out           = out;
        ctx.next_chunk    = 0;
        ctx.error         = 0;
        MUTEX_INIT(ctx.mutex);

        THREAD_T *tids = (THREAD_T *)malloc(threads * sizeof(THREAD_T));
        if (!tids) {
            MUTEX_DESTROY(ctx.mutex); free(out);
            PyErr_NoMemory(); goto meta_err;
        }

        for (int t = 0; t < threads; t++) {
            if (thread_create(&tids[t], decompress_worker, &ctx) != 0) {
                for (int j = 0; j < t; j++) thread_join(tids[j]);
                free(tids); MUTEX_DESTROY(ctx.mutex); free(out);
                PyErr_SetString(PyExc_RuntimeError, "thread_create failed");
                goto meta_err;
            }
        }
        for (int t = 0; t < threads; t++) thread_join(tids[t]);
        free(tids);
        MUTEX_DESTROY(ctx.mutex);

        free(types); free(src_offsets); free(comp_sizes);
        free(dst_offsets); free(decomp_sizes);
        PyBuffer_Release(&view);

        if (ctx.error) {
            free(out);
            PyErr_SetString(PyExc_RuntimeError, "Decompression failed");
            return NULL;
        }

        PyObject *result = PyBytes_FromStringAndSize(
            (const char *)out, (Py_ssize_t)original_size);
        free(out);
        return result;
    }

meta_err:
    free(types); free(src_offsets); free(comp_sizes);
    free(dst_offsets); free(decomp_sizes);
    PyBuffer_Release(&view);
    return NULL;
}

/* ── compress_bound ─────────────────────────────────────────────────────── */

static PyObject *py_compress_bound(PyObject *self, PyObject *args) {
    Py_ssize_t size;
    if (!PyArg_ParseTuple(args, "n", &size)) return NULL;
    size_t n = ((size_t)size + CHUNK_SIZE - 1) / CHUNK_SIZE;
    if (n == 0) n = 1;
    return PyLong_FromSize_t(
        MAGIC_LEN + 8 + 4 + n * (1 + 4 + HUF_COMPRESSBOUND(CHUNK_SIZE)));
}

/* ── module ─────────────────────────────────────────────────────────────── */

static PyMethodDef Huf0Methods[] = {
    {"compress", (PyCFunction)py_compress,
     METH_VARARGS | METH_KEYWORDS,
     "compress(data, threads=0) -> bytes\n\n"
     "Compress bytes using Huff0 (128 KB chunks, parallel).\n"
     "threads=0 uses all available CPU cores."},

    {"decompress", (PyCFunction)py_decompress,
     METH_VARARGS | METH_KEYWORDS,
     "decompress(data, threads=0) -> bytes\n\n"
     "Decompress data produced by huf0.compress().\n"
     "threads=0 uses all available CPU cores."},

    {"compress_bound", py_compress_bound, METH_VARARGS,
     "compress_bound(size) -> int\n\n"
     "Return worst-case compressed size for an input of given length."},

    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef huf0module = {
    PyModuleDef_HEAD_INIT, "_huf0_core",
    "Huff0 lossless compression with cross-platform thread parallelism",
    -1, Huf0Methods
};

PyMODINIT_FUNC PyInit__huf0_core(void) {
    return PyModule_Create(&huf0module);
}