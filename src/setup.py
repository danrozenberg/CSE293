from Cython.Build import cythonize
try:
    from setuptools import setup
    from setuptools import Extension
except ImportError:
    from distutils.core import setup
    from distutils.extension import Extension

extensions = [Extension("*", ["*.pyx"])]

setup(
    name="MyApp",
    ext_modules=cythonize(extensions),
)


