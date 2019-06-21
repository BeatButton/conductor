from os import path

from setuptools import find_packages, setup

here = path.abspath(path.dirname(__file__))

with open(path.join(here, "requirements.txt"), encoding="utf-8") as fp:
    install_requires = list(fp)

with open(path.join(here, "README.md"), encoding="utf-8") as fp:
    long_description = fp.read()

setup(
    name="conductor",
    version="0.1.0",
    description="A Python based task scheduler",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Tall Tree Administrators",
    packages=find_packages(),
    python_requires=">=3.7,<4"
    install_requires=install_requires,
)
