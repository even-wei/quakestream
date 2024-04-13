from setuptools import find_packages, setup

setup(
    name="quakestream",
    packages=find_packages(exclude=["quakestream_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
