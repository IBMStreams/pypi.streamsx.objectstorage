from unittest import TestCase

import streamsx.objectstorage as objectstorage
import os

from streamsx.topology.topology import Topology
from streamsx.topology.tester import Tester
from streamsx.topology.schema import CommonSchema, StreamSchema
import streamsx.rest as sr

import datetime
import time
import uuid
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

class StringData(object):
    def __init__(self, prefix, count, delay=True):
        self.prefix = prefix
        self.count = count
        self.delay = delay
    def __call__(self):
        if self.delay:
            time.sleep(5)
        for i in range(self.count):
            yield self.prefix + '_' + str(i)

class TestCOS(TestCase):

    @classmethod
    def setUpClass(self):
        # start streams service
        connection = sr.StreamingAnalyticsConnection()
        service = connection.get_streaming_analytics()
        result = service.start_instance()

    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)
        self.bucket = os.environ["COS_BUCKET"]
        self.endpoint='s3.private.us-south.cloud-object-storage.appdomain.cloud'

    def test_parquet(self):
        n = 5000
        topo = Topology()
        uid = str(uuid.uuid4())
        s = topo.source(StringData(uid, n)).as_string()
        objectstorage.write_parquet(s, self.bucket, self.endpoint, 'test%OBJECTNUM.parquet', time_per_object=5)
        
        scanned_objects = objectstorage.scan(topo, self.bucket, self.endpoint, 'test0.parquet')
        scanned_objects.print()

        tester = Tester(topo)
        tester.run_for(60)
        tester.tuple_count(scanned_objects, 1, exact=False)
        tester.test(self.test_ctxtype, self.test_config, always_collect_logs=True)

    def test_string(self):
        n = 5000
        topo = Topology()
        uid = str(uuid.uuid4())
        s = topo.source(StringData(uid, n)).as_string()
        objectstorage.write(s, self.bucket, self.endpoint, 'test%OBJECTNUM.txt')
        
        scanned_objects = objectstorage.scan(topo, bucket=self.bucket, endpoint=self.endpoint, pattern='test0.txt')
        scanned_objects.print()

        tester = Tester(topo)
        tester.run_for(60)
        tester.tuple_count(scanned_objects, 1, exact=False)
        tester.test(self.test_ctxtype, self.test_config, always_collect_logs=True)   

    def test_string_header(self):
        n = 5000
        topo = Topology()
        uid = str(uuid.uuid4())
        s = topo.source(StringData(uid, n)).as_string()
        objectstorage.write(s, self.bucket, self.endpoint, 'test%OBJECTNUM.txt', header="TEST_HEADER_ROW")
        
        scanned_objects = objectstorage.scan(topo, bucket=self.bucket, endpoint=self.endpoint, pattern='test0.txt')
        scanned_objects.print()

        tester = Tester(topo)
        tester.run_for(60)
        tester.tuple_count(scanned_objects, 1, exact=False)
        tester.test(self.test_ctxtype, self.test_config, always_collect_logs=True)  

    def test_timedelta(self):
        n = 5000
        topo = Topology()
        uid = str(uuid.uuid4())
        s = topo.source(StringData(uid, n)).as_string()
        objectstorage.write(s, self.bucket, self.endpoint, object='test%OBJECTNUM.time', time_per_object=datetime.timedelta(minutes=1))
        
        scanned_objects = objectstorage.scan(topo, bucket=self.bucket, endpoint=self.endpoint, pattern='test0.time')
        scanned_objects.print()

        tester = Tester(topo)
        tester.run_for(120)
        tester.tuple_count(scanned_objects, 1, exact=False)
        tester.test(self.test_ctxtype, self.test_config, always_collect_logs=True)   

    def test_hello_world(self):
        topo = Topology('ObjectStorageHelloWorld')
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

