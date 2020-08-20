# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2018

"""
Overview
++++++++

`IBM® Cloud Object Storage <https://www.ibm.com/cloud/object-storage>`_ (COS) makes it possible to store practically limitless amounts of data, simply and cost effectively. It is commonly used for data archiving and backup, web and mobile applications, and as scalable, persistent storage for analytics.

Cloud Object Storage or any other S3 compatible object storage can be used.

This module allows a Streams application to create objects in parquet format :py:class:`WriteParquet <WriteParquet>` or
to write string messages with :py:class:`Write <Write>` from a stream
of tuples.
Objects can be listed with :py:class:`Scan <Scan>` and read with :py:class:`Read <Read>`.

Credentials
+++++++++++

Select one of the following options to define your Cloud Object Storage credentials:

* Streams `application configuration <https://ibmstreams.github.io/streamsx.objectstorage/doc/spldoc/html/tk$com.ibm.streamsx.objectstorage/op$com.ibm.streamsx.objectstorage$ObjectStorageScan$4.html>`_
* Setting the Cloud Object Storage service credentials JSON as dict to the ``credentials`` `parameter <https://ibmstreams.github.io/streamsx.objectstorage/doc/spldoc/html/tk$com.ibm.streamsx.objectstorage/op$com.ibm.streamsx.objectstorage$ObjectStorageScan$3.html>`_ of the functions.

By default an application configuration named `cos` is used,
a different configuration name can be specified using the ``credentials``
parameter to :py:class:`Write`, :py:class:`WriteParquet`, :py:class:`Scan` or :py:class:`Read`.

In addition to IAM token-based authentication, it is also possible to authenticate using a signature created from a pair of access and secret keys. 
Provide the HMAC keys with the ``credentials`` parameter as dictionary, for example:: 

    credentials = {}
    credentials['access_key_id'] = '7exampledonotusea6440da12685eee02'
    credentials['secret_access_key'] = '8not8ed850cddbece407exampledonotuse43r2d2586'


Endpoints
+++++++++

It is required that you `create a bucket <https://console.bluemix.net/docs/services/cloud-object-storage/getting-started.html#create-buckets>`_ before launching an application using this module.

When running the application in a **Streaming Analytics service** instance, it is recommended, for best performance, to create a bucket with:

* Resiliency: `regional`

* Location: Near your Streaming Analytics service, for example `us-south`

* Storage class: `Standard`

With these setting above it is recommended to use the private endpoint for the US-South region::

    endpoint='s3.private.us-south.cloud-object-storage.appdomain.cloud'

**Note:**

* *Use public endpoints to point your application that are hosted outside of the IBM cloud.*
* *Use cross-region endpoints for buckets creation with cross-region resiliency.*
* *Set the URL to object storage service with the* ``endpoint`` *parameter.*

Find the list of endpoints and the endpoint description here: `IBM® Cloud Object Storage Endpoints <https://console.bluemix.net/docs/services/cloud-object-storage/basics/endpoints.html>`_

To access any other Amazon S3 compatible object storage server you need set the ``endpoint`` parameter, for example the MinIO server running at `<https://play.min.io:9000>`_ needs a value for the endpoint parameter like below::

    endpoint='play.min.io:9000'

Sample
++++++

A simple hello world example of a Streams application writing string messages to
an object. Scan for created object and read the content::

    from streamsx.topology.topology import *
    from streamsx.topology.schema import CommonSchema
    from streamsx.topology.context import submit
    import streamsx.objectstorage as cos

    topo = Topology('ObjectStorageHelloWorld')

    to_cos = topo.source(['Hello', 'World!'])
    to_cos = to_cos.as_string()

    # sample bucket with resiliency "regional" and location "us-south"
    bucket = 'streamsx-py-sample'
    # US-South region private endpoint
    endpoint='s3.private.us-south.cloud-object-storage.appdomain.cloud'
    # provide COS credentials as dict
    credentials = json.loads(your_cos_json)
    
    # Write a stream to COS
    to_cos.for_each(cos.Write(bucket=bucket, endpoint=endpoint, object='/sample/hw%OBJECTNUM.txt', credentials=credentials))

    scanned = topo.source(cos.Scan(bucket=bucket, endpoint=endpoint, directory='/sample', credentials=credentials))
         
    # read text file line by line
    r = scanned.map(cos.Read(bucket=bucket, endpoint=endpoint, credentials=credentials))
    
    # print each line (tuple)
    r.print()

    submit('STREAMING_ANALYTICS_SERVICE', topo)
    # Use for IBM Streams including IBM Cloud Pak for Data
    # submit ('DISTRIBUTED', topo)

"""

__version__='1.5.4'

__all__ = ['Scan', 'Read', 'Write', 'WriteParquet', 'download_toolkit', 'configure_connection', 'scan', 'read', 'write', 'write_parquet']
from streamsx.objectstorage._objectstorage import Scan, Read, Write, WriteParquet, download_toolkit, configure_connection, scan, read, write, write_parquet

