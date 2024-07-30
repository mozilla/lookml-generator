"""Installation for lookml-generator."""

# -*- coding: utf-8 -*-

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from setuptools import find_packages, setup

readme = open("README.md").read()

setup(
    name="lookml-generator",
    python_requires=">=3.10.0",
    version="0.0.0",
    description="Generator LookML to represent Mozilla data.",
    long_description=readme,
    long_description_content_type="text/markdown",
    author="Frank Bertsch",
    author_email="frank@mozilla.com",
    url="https://github.com/mozilla/lookml-generator",
    packages=find_packages(include=["generator", "generator.*"]),
    package_dir={"lookml-generator": "generator"},
    entry_points={
        "console_scripts": [
            "lookml-generator=generator.__main__:main",
        ]
    },
    include_package_data=True,
    package_data={"generator": ["dashboards/templates/*.lkml"]},
    zip_safe=False,
    keywords="lookml-generator",
    classifiers=[
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
    ],
)
