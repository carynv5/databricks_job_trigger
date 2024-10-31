from setuptools import setup, find_packages
import survey_processing

setup(
    name='survey_processing',
    version=survey_processing.__version__,
    author=survey_processing.__author__,
    author_email='cary.greenwood@nv5.com',
    packages=find_packages(),
    python_requires='>=3.8',
    # entry_points={
    #     'console_scripts': [
    #         'survey-process=survey_processing.__main__:main',
    #     ],
    # },
    install_requires=[
        'pyyaml>=6.0',
    ]
)