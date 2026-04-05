import os
import subprocess

from setuptools import Extension, find_packages, setup


def update_submodules():
    if os.path.exists(".git"):
        try:
            subprocess.check_call(
                ["git", "submodule", "update", "--init", "--recursive"]
            )
        except subprocess.CalledProcessError as e:
            print(f"Failed to update submodules: {e}")
            raise


update_submodules()

FSE_LIB = "include/FiniteStateEntropy/lib"

huf0_core_extension = Extension(
    "huf0._huf0_core",
    sources=[
        "csrc/huf0_module.c",
        f"{FSE_LIB}/huf_compress.c",
        f"{FSE_LIB}/huf_decompress.c",
        f"{FSE_LIB}/entropy_common.c",
        f"{FSE_LIB}/fse_compress.c",
        f"{FSE_LIB}/fse_decompress.c",
        f"{FSE_LIB}/hist.c",
        f"{FSE_LIB}/debug.c",
    ],
    include_dirs=[FSE_LIB, "csrc/"],
    extra_compile_args=["-O3", "-Wall"],
    extra_link_args=["-O3"],
)

setup(
    name="huf0",
    version="0.1.0",
    author="",
    description="Huff0 lossless compression with Python bindings",
    long_description=open("README.md").read() if os.path.exists("README.md") else "",
    long_description_content_type="text/markdown",
    packages=find_packages(include=["huf0", "huf0.*"]),
    python_requires=">=3.8",
    extras_require={
        "dev": [
            "black==24.8.0",
            "flake8==7.3.0",
            "isort==8.0.1",
            "pytest==9.0.2",
        ]
    },
    ext_modules=[huf0_core_extension],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
