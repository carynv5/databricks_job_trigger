from setuptools import setup, find_packages

setup(
    name="dbfs_finder",
    version="0.1.0",
    packages=find_packages("src"),
    package_dir={"": "src"},
    python_requires=">=3.12",
    install_requires=[
        "requests>=2.31.0",
        "python-dotenv>=1.0.0",
        "typing-extensions>=4.7.0",
    ],
    entry_points={
        "console_scripts": [
            "dbfs-finder=dbfs_finder.cli:main",
        ],
    },
)
