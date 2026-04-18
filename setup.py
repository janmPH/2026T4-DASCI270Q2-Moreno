from setuptools import setup, find_packages

setup(
    name="sales-pipeline",
    packages=find_packages(),
    install_requires=[
        "dagster",
        "pandas",
    ],
)
