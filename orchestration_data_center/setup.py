from setuptools import find_packages, setup

setup(
    name="orchestration_data_center",
    version="0.0.1",
    packages=find_packages(),
    package_data={
        "orchestration_data_center": [
            "dbt-project/**/*",
        ],
    },
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-dbt",
        "dbt-clickhouse<1.9",
    ],
    extras_require={
        "dev": [
            "dagster-webserver",
        ]
    },
)