from setuptools import setup, find_packages

setup(
    name="survey_processing",
    version="0.1.0",
    description="A Databricks bundle for processing survey data",
    author="Cary Greenwood",
    author_email="cary.greenwood@nv5.com",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[
        "pandas>=2.0.0",
        "pyspark>=3.4.0",
        "pyyaml>=6.0",
        "databricks-sdk>=0.8.0",
        "delta-spark>=2.4.0",
        "pytest>=7.0.0",
    ],
    extras_require={
        "dev": [
            "black>=23.0.0",
            "flake8>=6.0.0",
            "mypy>=1.0.0",
            "pytest-cov>=4.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "survey-process=survey_processing.main:main",
        ],
    },
    include_package_data=True,
    # Add package data files
    package_data={
        "survey_processing": [
            "data/lookup_tables/*.csv",
            "conf/**/*.yml",
        ],
    },
    # Add additional files to include
    data_files=[
        ("conf/dev", ["conf/dev/config.yml"]),
        ("conf/prod", ["conf/prod/config.yml"]),
    ]
)