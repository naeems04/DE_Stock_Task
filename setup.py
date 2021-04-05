from codecs import open as codecs_open
from setuptools import setup, find_packages

# Get the long description from the relevant file
with codecs_open('ReadDMe.md', encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='stockstreaming',
    version='0.0.1',
    packages=find_packages(include=['stockstreaming', 'stockstreaming.*'],
                           exclude=['*.tests.*', '*.tests']),
    url='',
    license='',
    description='Data Engineering Assignment',
    long_description=long_descrsiption
)