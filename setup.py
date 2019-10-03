import os
from setuptools import setup, find_packages


README_FILE = os.path.join(os.path.dirname(__file__), 'README.md')

setup(
    name="pandas_access",
    version="0.0.1",
    packages=find_packages(),
    license="License :: OSI Approved :: MIT License",
    author="John Bjorn Nelson",
    author_email="jbn@abreka.com",
    description="A tiny, subprocess-based tool for reading a MS Access database(.rdb) as a Pandas DataFrame.",
    long_description=open(README_FILE).read(),
    data_files=['README.md'],
    url="https://github.com/jbn/pandas_access"
)