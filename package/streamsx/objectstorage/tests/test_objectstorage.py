from unittest import TestCase

import streamsx.objectstorage as objectstorage

from streamsx.topology.topology import Topology
from streamsx.topology.tester import Tester
from streamsx.topology.schema import CommonSchema, StreamSchema
import streamsx.spl.op as op
import streamsx.spl.toolkit
import streamsx.rest as sr
import os
import datetime
import time
import json

##
## Test assumptions
##
## Streaming analytics service has:
##    application config 'cos' configured for Cloud Object Storage
##
## Cloud Object Storage has:
##
##    bucket given in location us-south and resiliency regional with the 'COS_BUCKET' environment variable

class TestParams(TestCase):
    def test_params(self):
        topo = Topology('ObjectStorageHelloWorld')
        to_cos = topo.source(['Hello', 'World!'])
        to_cos = to_cos.as_string()
        objectstorage.write(to_cos, 'streams-bucket', 's3.private.us-south.cloud-object-storage.appdomain.cloud', '/sample/test%OBJECTNUM.txt')
        objectstorage.write_parquet(to_cos, 'streams-bucket', 's3.private.us-south.cloud-object-storage.appdomain.cloud', 'test%OBJECTNUM.parquet')
        objectstorage.write(to_cos, bucket='streams-bucket', endpoint='s3.private.us-south.cloud-object-storage.appdomain.cloud', object='/sample/test%OBJECTNUM.txt', time_per_object=30)
        objectstorage.write_parquet(to_cos, bucket='streams-bucket', endpoint='s3.private.us-south.cloud-object-storage.appdomain.cloud', object='test%OBJECTNUM.parquet', time_per_object=30.5)
        scanned = objectstorage.scan(topo, bucket='streams-bucket', endpoint='s3.private.us-south.cloud-object-storage.appdomain.cloud', pattern='test[0-9]*\\.txt$', directory='/sample')
        r = objectstorage.read(scanned, bucket='streams-bucket', endpoint='s3.private.us-south.cloud-object-storage.appdomain.cloud')

    def test_bad_time(self):
        topo = Topology()
        to_cos = topo.source(['Bad', 'Time'])
        to_cos = to_cos.as_string()
        
        self.assertRaises(TypeError, objectstorage.write_parquet, to_cos, 'streams-bucket', 's3.private.us-south.cloud-object-storage.appdomain.cloud', 'test%OBJECTNUM.parquet',  '1')   
        self.assertRaises(ValueError, objectstorage.write_parquet, to_cos, 'streams-bucket', 's3.private.us-south.cloud-object-storage.appdomain.cloud', 'test%OBJECTNUM.parquet', datetime.timedelta(milliseconds=1))
        self.assertRaises(ValueError, objectstorage.write_parquet, to_cos, 'streams-bucket', 's3.private.us-south.cloud-object-storage.appdomain.cloud', 'test%OBJECTNUM.parquet', 0.1)
        self.assertRaises(ValueError, objectstorage.write, to_cos, 'streams-bucket', 's3.private.us-south.cloud-object-storage.appdomain.cloud', 'test%OBJECTNUM.parquet', datetime.timedelta(milliseconds=100))
        self.assertRaises(ValueError, objectstorage.write, to_cos, 'streams-bucket', 's3.private.us-south.cloud-object-storage.appdomain.cloud', 'test%OBJECTNUM.parquet', 0.9)


class TestCOS(TestCase):

    @classmethod
    def setUpClass(self):
        self.bucket = os.environ["COS_BUCKET"]
        self.endpoint=os.environ["COS_ENDPOINT"]

    def test_parquet(self):
        topo = Topology()
        if self.objectstorage_toolkit_home is not None:
            streamsx.spl.toolkit.add_toolkit(topo, self.objectstorage_toolkit_home)
        to_cos = topo.source(['Hello', 'World!'])
        to_cos = to_cos.as_string()
        objectstorage.write_parquet(to_cos, self.bucket, self.endpoint, 'test%OBJECTNUM.parquet', time_per_object=5)
        
        scanned_objects = objectstorage.scan(topo, self.bucket, self.endpoint, 'test0.parquet')
        scanned_objects.print()

        tester = Tester(topo)
        tester.run_for(60)
        tester.tuple_count(scanned_objects, 1, exact=False)
        tester.test(self.test_ctxtype, self.test_config, always_collect_logs=True)

    def test_string(self):
        topo = Topology()
        if self.objectstorage_toolkit_home is not None:
            streamsx.spl.toolkit.add_toolkit(topo, self.objectstorage_toolkit_home)
        to_cos = topo.source(['Hello', 'World!'])
        to_cos = to_cos.as_string()
        objectstorage.write(to_cos, self.bucket, self.endpoint, 'test%OBJECTNUM.txt')
        
        scanned_objects = objectstorage.scan(topo, bucket=self.bucket, endpoint=self.endpoint, pattern='test0.txt')
        scanned_objects.print()

        tester = Tester(topo)
        tester.run_for(60)
        tester.tuple_count(scanned_objects, 1, exact=False)
        tester.test(self.test_ctxtype, self.test_config, always_collect_logs=True)   

    def test_string_header(self):
        topo = Topology()
        if self.objectstorage_toolkit_home is not None:
            streamsx.spl.toolkit.add_toolkit(topo, self.objectstorage_toolkit_home)
        to_cos = topo.source(['Hello', 'World!'])
        to_cos = to_cos.as_string()
        objectstorage.write(to_cos, self.bucket, self.endpoint, 'test%OBJECTNUM.txt', header="TEST_HEADER_ROW")
        
        scanned_objects = objectstorage.scan(topo, bucket=self.bucket, endpoint=self.endpoint, pattern='test0.txt')
        scanned_objects.print()

        tester = Tester(topo)
        tester.run_for(60)
        tester.tuple_count(scanned_objects, 1, exact=False)
        tester.test(self.test_ctxtype, self.test_config, always_collect_logs=True)  

    def test_timedelta(self):
        topo = Topology()
        if self.objectstorage_toolkit_home is not None:
            streamsx.spl.toolkit.add_toolkit(topo, self.objectstorage_toolkit_home)
        to_cos = topo.source(['Hello', 'World!'])
        to_cos = to_cos.as_string()
        objectstorage.write(to_cos, self.bucket, self.endpoint, object='test%OBJECTNUM.time', time_per_object=datetime.timedelta(minutes=1))
        
        scanned_objects = objectstorage.scan(topo, bucket=self.bucket, endpoint=self.endpoint, pattern='test0.time')
        scanned_objects.print()

        tester = Tester(topo)
        tester.run_for(120)
        tester.tuple_count(scanned_objects, 1, exact=False)
        tester.test(self.test_ctxtype, self.test_config, always_collect_logs=True)   

    def test_hello_world(self):
        topo = Topology('ObjectStorageHelloWorld')
        if self.objectstorage_toolkit_home is not None:
            streamsx.spl.toolkit.add_toolkit(topo, self.objectstorage_toolkit_home)
        to_cos = topo.source(['Hello', 'World!'])
        to_cos = to_cos.as_string()
        objectstorage.write(to_cos, self.bucket, self.endpoint, '/sample/hw%OBJECTNUM.txt')

        scanned = objectstorage.scan(topo, bucket=self.bucket, endpoint=self.endpoint, directory='/sample')
        r = objectstorage.read(scanned, bucket=self.bucket, endpoint=self.endpoint)
        r.print()
        
        tester = Tester(topo)
        tester.run_for(60)
        tester.tuple_count(r, 2, exact=True) # expect two lines 1:hello 2:World!
        tester.test(self.test_ctxtype, self.test_config, always_collect_logs=True)  

    def test_credentials(self):
        cred_file = os.environ['COS_IAM_CREDENTIALS']
        print("COS IAM credentials file:" + cred_file)
        with open(cred_file) as data_file:
            credentials = json.load(data_file)

        topo = Topology('ObjectStorageHelloWorldWithCreds')
        if self.objectstorage_toolkit_home is not None:
            streamsx.spl.toolkit.add_toolkit(topo, self.objectstorage_toolkit_home)
        to_cos = topo.source(['Hello', 'World!'])
        to_cos = to_cos.as_string()
        objectstorage.write(to_cos, self.bucket, self.endpoint, '/c/hw_%OBJECTNUM.txt', credentials=credentials)

        scanned = objectstorage.scan(topo, bucket=self.bucket, endpoint=self.endpoint, directory='/c', credentials=credentials)
        r = objectstorage.read(scanned, bucket=self.bucket, endpoint=self.endpoint, credentials=credentials)
        r.print()
        
        tester = Tester(topo)
        tester.run_for(60)
        tester.tuple_count(r, 2, exact=True) # expect two lines 1:hello 2:World!
        tester.test(self.test_ctxtype, self.test_config, always_collect_logs=True) 

class TestDistributed(TestCOS):

    @classmethod
    def setUpClass(self):
        super().setUpClass()

    def setUp(self):
        Tester.setup_distributed(self)
        self.objectstorage_toolkit_home = os.environ["COS_TOOLKIT_HOME"]
        self.test_config[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False  


class TestStreamingAnalytics(TestCOS):

    @classmethod
    def setUpClass(self):
        # start streams service
        connection = sr.StreamingAnalyticsConnection()
        service = connection.get_streaming_analytics()
        result = service.start_instance()
        super().setUpClass()

    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=False)
        self.objectstorage_toolkit_home = os.environ["COS_TOOLKIT_HOME"]


class TestStreamingAnalyticsRemote(TestStreamingAnalytics):

    @classmethod
    def setUpClass(self):
        super().setUpClass()

    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)
        self.objectstorage_toolkit_home = None


