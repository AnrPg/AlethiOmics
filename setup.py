from setuptools import setup, find_packages
import pathlib

def _parse_requirements(fname="requirements.txt"):
    lines = (pathlib.Path(__file__).parent / fname).read_text().splitlines()
    return [l.strip() for l in lines if l.strip() and not l.startswith("#")]

setup(
    name="alethiomics",
    version="0.1.0",
    # discover only real packages under etl/, wh/, visualization/, etc.
    packages=find_packages(),
    # tell it about your one-off main.py
    py_modules=["main"],
    python_requires=">=3.8",
    install_requires=_parse_requirements(),
    entry_points={
        "console_scripts": [
            # point the script at the standalone module main.py
            "alethiomics = main:main",
        ],
    },

    author="Regas Apostolos-Nikolaos",
    author_email="regas.apn@gmail.com",
    description="Gut–Brain Organoid Data-Warehouse ETL pipeline",
    url="https://github.com/AnrPg/AlethiOmics",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
