from distutils.core import setup
from Cython.Build import cythonize
setup(ext_modules = cythonize("graph_manager.pyx"))
setup(ext_modules = cythonize("connect_workers.pyx"))

