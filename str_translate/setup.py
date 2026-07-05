#!/usr/bin/env python3
# encoding: utf-8

from glob import glob
from itertools import chain
from setuptools import setup, Extension, find_packages
from setuptools.command.sdist import sdist
from setuptools.command.bdist_wheel import bdist_wheel


PACKAGE_DIR = "str_translate"


def clean_temporary_files():
    from shutil import rmtree
    for dir_ in chain(("build",), glob("*.egg-info")):
        rmtree(dir_, ignore_errors=True)
    from os import unlink
    for file in glob(f"{PACKAGE_DIR}/**/*.c", recursive=True):
        unlink(file)


class CustomSdist(sdist):
    """当执行 poetry build 生成 .tar.gz (sdist) 结束时触发"""
    def run(self):
        try:
            super().run()
        finally:
            clean_temporary_files()


class CustomBdistWheel(bdist_wheel):
    """当执行 poetry build 生成 .whl (wheel) 结束时触发"""
    def run(self):
        try:
            super().run()
        finally:
            clean_temporary_files()


USE_CYTHON = False
pyx_files = [
    f for f in glob(f"{PACKAGE_DIR}/**/*.pyx", recursive=True)
    if "build" not in f and "dist" not in f
]
if pyx_files:
    try:
        from Cython.Build import cythonize
        USE_CYTHON = True
    except ImportError:
        pass

if USE_CYTHON:
    extensions = cythonize(
        pyx_files, 
        compiler_directives={"language_level": "3"}, 
    )
else:
    from os.path import splitext, sep
    extensions = [
         Extension(
            name=splitext(f)[0].replace(sep, "."), 
            sources=[f], 
        )
        for f in glob(f"{PACKAGE_DIR}/**/*.c", recursive=True)
        if "build" not in f and "dist" not in f
    ]

setup(
    name=PACKAGE_DIR, 
    version="0.0.1", 
    packages=find_packages(), 
    ext_modules=extensions, 
    zip_safe=False, 
    cmdclass={
        "sdist": CustomSdist,
        "bdist_wheel": CustomBdistWheel,
    }
)
