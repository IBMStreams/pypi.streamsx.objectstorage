# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2018

"""
Overview
++++++++

`IBM® Cloud Object Storage <https://www.ibm.com/cloud/object-storage>`_ (COS) makes it possible to store practically limitless amounts of data, simply and cost effectively. It is commonly used for data archiving and backup, web and mobile applications, and as scalable, persistent storage for analytics.

Cloud Object Storage or any other S3 compatible object storage can be used.

This module allows a Streams application to create objects in parquet format :py:func:`write_parquet <write_parquet>` or
to write string messages with :py:func:`write <write>` from a stream
of tuples.
Objects can be listed with :py:func:`scan <scan>` and read with :py:func:`read <read>`.

Credentials
+++++++++++

Select one of the following options to define your Cloud Object Storage credentials:

* Streams `application configuration <https://ibmstreams.github.io/streamsx.objectstorage/doc/spldoc/html/tk$com.ibm.streamsx.objectstorage/op$com.ibm.streamsx.objectstorage$ObjectStorageScan$4.html>`_
* Setting the Cloud Object Storage service credentials JSON directly to the ``credentials`` `parameter <https://ibmstreams.github.io/streamsx.objectstorage/doc/spldoc/html/tk$com.ibm.streamsx.objectstorage/op$com.ibm.streamsx.objectstorage$ObjectStorageScan$3.html>`_ of the functions.

By default an application configuration named `cos` is used,
a different configuration name can be specified using the ``credentials``
parameter to :py:func:`write`, :py:func:`write_parquet`, :py:func:`scan` or :py:func:`read`.

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

To access any other Amazon S3 compatible object storage server you need set the ``endpoint`` parameter, for example the MinIO server running at `<https://play.min.io:9000>`_::

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
    
    # Write a stream to COS
    cos.write(to_cos, bucket, endpoint, '/sample/hw%OBJECTNUM.txt')

    scanned = cos.scan(topo, bucket=bucket, endpoint=endpoint, directory='/sample')
    
    # read text file line by line
    r = cos.read(scanned, bucket=bucket, endpoint=endpoint)
    
    # print each line (tuple)
    r.print()

    submit('STREAMING_ANALYTICS_SERVICE', topo)
    # Use for IBM Streams including IBM Cloud Pak for Data
    # submit ('DISTRIBUTED', topo)

"""

__version__='1.4.2'

__all__ = ['download_toolkit', 'write_parquet', 'scan', 'read', 'write', 'configure_connection']
from streamsx.objectstorage._objectstorage import download_toolkit, write_parquet, scan, read, write, configure_connection
