# setup.py
from setuptools import setup, find_packages

setup(
    name='pages-viewed-streaming',
    version='1.0.0',
    packages=find_packages(),
    install_requires=[
        'apache-beam[gcp]==2.53.0',
        'google-cloud-pubsub==2.18.4',
    ],
)
