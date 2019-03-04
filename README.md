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
* http://streamsxobjectstorage.readthedocs.io/

## Version update

To change the version information of the Python package, edit following files:

- ./package/docs/source/conf.py
- ./package/streamsx/objectstorage/\_\_init\_\_.py

When the development status changes, edit the *classifiers* in

- ./package/setup.py

When the documented sample must be changed, change it here:

- ./package/streamsx/objectstorage/\_\_init\_\_.py
- ./package/DESC.txt

## Test

When running in Streaming analytics service you may select a private endpoint to access your bucket, e.g. bucket given in location us-south and resiliency regional.
When running in your local Streams instance you should select a public endpoint.

```
export COS_BUCKET=<YOUR_BUCKET>
export COS_ENDPOINT=<ENDPOINT_FOR_YOUR_BUCKET>
```

When using local build (e.g. not forcing remote build), then you need to specifiy the toolkit location, for example:

    export COS_TOOLKIT_HOME=$STREAMS_INSTALL/toolkits/com.ibm.streamsx.objectstorage


### Streaming Analytics service

Package can be tested with TopologyTester using the [Streaming Analytics](https://www.ibm.com/cloud/streaming-analytics) service.

Run the test with:

    ant test-sas

or

```
cd package
python3 -u -m unittest streamsx.objectstorage.tests.test_objectstorage.TestStreamingAnalytics
```

#### Remote build

For using the toolkit from the build service (**force_remote_build**) run the test with:

Run the test with:

    ant test-sas-remote

or

```
cd package
python3 -u -m unittest streamsx.objectstorage.tests.test_objectstorage.TestStreamingAnalyticsRemote
```

### Local Streams instance

Package can be tested with TopologyTester using a local and running Streams domain.
Make sure that the streams environment is set, the domain and instance is running and the environment variables:
STREAMS_USERNAME
STREAMS_PASSWORD
are setup.

Run the test with:

    ant test

or

```
cd package
python3 -u -m unittest streamsx.objectstorage.tests.test_objectstorage.TestDistributed
```

