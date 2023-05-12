import os

from setuptools import setup, find_packages

from eswrap import _version_from_git_describe

# The directory containing this file
HERE = os.path.abspath(os.path.dirname(__file__))

# The text of the README file
with open(os.path.join(HERE, "README.md")) as fid:
    README = fid.read()

with open(os.path.join(HERE, "requirements.txt")) as fid:
    REQS = fid.read().splitlines()


setup(
    name="eswrap",
    version=_version_from_git_describe(),
    packages=find_packages(exclude=("tests", "test_data")),
    url="",
    license="GNU General Public License v3.0",
    author="Paul Tikken",
    author_email="paul.tikken@gmail.com",
    description="Python wrapper for simple elasticsearch queries",
    long_description=README,
    long_description_content_type="text/markdown",
    package_data={"eswrap": ["LICENSE", "VERSION"]},
    include_package_data=True,
    classifiers=[
        "License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
    ],
    python_requires=">=3.10",
    install_requires=REQS,
)
