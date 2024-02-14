"""
Python Dependencies
"""

from setuptools import setup

dependencies = {
    "pyspark>=3.3.0, <=3.5.0",
    "delta-spark>=2.3.0, <=3.1.0",

}

setup(
    name='rezq',
    version='0.1.4',
    packages=['rezq'],
    python_requires='>=3.8',
    package_dir={"": "src"},
    install_requires=list(dependencies)
)
