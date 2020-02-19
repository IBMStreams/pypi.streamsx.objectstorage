from setuptools import setup
import streamsx.objectstorage
setup(
  name = 'streamsx.objectstorage',
  packages = ['streamsx.objectstorage'],
  include_package_data=True,
  version = streamsx.objectstorage.__version__,
  description = 'Object Storage integration for IBM Streams',
  long_description = open('DESC.txt').read(),
  author = 'IBM Streams @ github.com',
  author_email = 'hegermar@de.ibm.com',
  license='Apache License - Version 2.0',
  url = 'https://github.com/IBMStreams/streamsx.objectstorage',
  keywords = ['streams', 'ibmstreams', 'streaming', 'analytics', 'streaming-analytics', 'cos', 'objectstorage'],
  classifiers = [
    'Development Status :: 5 - Production/Stable',
    'License :: OSI Approved :: Apache Software License',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
  ],
  install_requires=['streamsx>=1.12.10,<2.0', 'streamsx.toolkits'],
  
  test_suite='nose.collector',
  tests_require=['nose']
)
