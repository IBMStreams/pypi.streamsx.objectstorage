# Python streamsx.objectstorage package

This exposes SPL operators in the `com.ibm.streamsx.objectstorage` toolkit as Python methods.

Package is organized using standard packaging to upload to PyPi.

The package is uploaded to PyPi in the standard way:
```
cd package
python setup.py sdist bdist_wheel upload -r pypi
```
Note: This is done using the `ibmstreams` account at pypi.org and requires `.pypirc` file containing the credentials in your home directory.

Package details: https://pypi.python.org/pypi/streamsx.objectstorage

Documentation is using Sphinx and can be built locally using:
```
cd package/docs
make html
```
and viewed using
```
firefox python/package/docs/build/html/index.html
```

The documentation is also setup at `readthedocs.io`.

Documentation links:
* http://streamsxobjectstorage.readthedocs.io/en/pypackage

## Test

Package can be tested with TopologyTester using COS and Streaming Analytics service:
```
export COS_BUCKET=<YOUR_BUCKET>
cd package
python3 -u -m unittest streamsx.objectstorage.tests.test_objectstorage.TestCOS
```
