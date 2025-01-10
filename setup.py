from setuptools import setup, find_packages
import os


def readme():
    with open('README.md', encoding='utf-8') as f:
        return f.read()


version = os.environ.get('CI_COMMIT_TAG', f"1.0")


setup(
    name='database_tools',
    version=version,
    description='Common database tools - adapters, uploaders etc.',
    long_description=readme(),
    url='https://github.com/tenJnd/database-tools',
    author='Tomas.jnd',
    author_email='',
    packages=find_packages(exclude=('tests', 'docs')),
    python_requires='>=3.6',
    install_requires=[
        'SQLAlchemy>=1.3.12',
        'pandas',
        'numpy',
        'psycopg2-binary==2.9.3',
        'pybigquery==0.4.15',
        'regex>=2020.1.8'
    ],
)
