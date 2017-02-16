import os
from uuid import uuid4
from setuptools import setup
from pip.req import parse_requirements

install_reqs = parse_requirements('requirements.txt', session=uuid4())
reqs = [str(ir.req) for ir in install_reqs]

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "dervish",
    version = "0.0.10",
    author = "Luke Samaha",
    author_email = "lwsamaha@gmail.com",
    description = ("Demonstrates AWS kinesis consumers writing events to s3 and indexing them in dynamodb."),
    license = "BSD",
    keywords = "aws kinesis consumer dynamodb event",
    url = "http://packages.python.org/dervish",
    packages=['dervish'],
    install_requires=reqs,
    entry_points = {'console_scripts':['dervish = dervish.whirling:main']}
)

classifiers=[
    'Development Status :: 3 - Alpha',
    'Intended Audience :: Developers',
    'Topic :: Software Development :: Build Tools',
    'License :: OSI Approved :: BSD License',
    'Programming Language :: Python :: 2.7'
]
