from unittest import TestCase
import unittest

import streamsx.objectstorage as objectstorage

from streamsx.topology.topology import Topology
from streamsx.topology.tester import Tester
from streamsx.topology.schema import CommonSchema, StreamSchema
from streamsx.topology import context
import streamsx.spl.op as op
import streamsx.spl.toolkit
import streamsx.rest as sr
import os
import datetime
import time
import json
import random
import string
from subprocess import call, Popen, PIPE

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

    def test_credentials_as_dict(self):
        cred_file = os.environ['COS_IAM_CREDENTIALS']
        print("COS IAM credentials file:" + cred_file)
        with open(cred_file) as data_file:
            credentials = json.load(data_file)

        topo = Topology('test_credentials_as_dict')
        to_cos = topo.source(['Hello', 'World!'])
        to_cos = to_cos.as_string()
        objectstorage.write(to_cos, 'abc', 'xx', '/x/yz_%OBJECTNUM.txt', credentials=credentials)

        result = context.submit("TOOLKIT", topo.graph) # creates tk* directory
        print(' (TOOLKIT):' + str(result))


def _run_shell_command_line(command):
    process = Popen(command, universal_newlines=True, shell=True, stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()
    return stdout, stderr, process.returncode

def _streams_install_env_var():
    result = True
    try:
        os.environ['STREAMS_INSTALL']
    except KeyError: 
        result = False
    return result

def _cp4d_url_env_var():
    result = True
    try:
        os.environ['CP4D_URL']
    except KeyError: 
        result = False
    return result

def _cos_iam_env_var():
    result = True
    try:
        os.environ['COS_IAM_CREDENTIALS']
    except KeyError: 
        result = False
    return result

def _cos_hmac_env_var():
    result = True
    try:
        os.environ['COS_HMAC_CREDENTIALS']
    except KeyError: 
        result = False
    return result

def _minio_credentials():
    result = True
    try:
        os.environ['MINIO_HMAC_CREDENTIALS']
    except KeyError: 
        result = False
    return result

class TestDistributed(TestCase):

    @classmethod
    def setUpClass(self):
        self.bucket = os.environ["COS_BUCKET"]
        self.endpoint=os.environ["COS_ENDPOINT"]

    def setUp(self):
        Tester.setup_distributed(self)
        self.objectstorage_toolkit_home = os.environ["COS_TOOLKIT_HOME"]
        self.test_config[context.ConfigParams.SSL_VERIFY] = False  

    def _get_credentials(self):
        cred_file = os.environ['COS_IAM_CREDENTIALS']
        with open(cred_file) as data_file:
            credentials = json.load(data_file)
        return credentials

    def _get_hmac_credentials(self):
        cred_file = os.environ['COS_HMAC_CREDENTIALS']
        with open(cred_file) as data_file:
            credentials = json.load(data_file)
        return credentials

    def _run_tester_app(self, name, dir, credentials):
        topo = Topology(name)
        if self.objectstorage_toolkit_home is not None:
            streamsx.spl.toolkit.add_toolkit(topo, self.objectstorage_toolkit_home)
        to_cos = topo.source(['Hello', 'World!'])
        to_cos = to_cos.as_string()
        to_cos.for_each(objectstorage.Write(self.bucket, self.endpoint, dir+'/hw%OBJECTNUM.txt', credentials=credentials))

        scanned = topo.source(objectstorage.Scan(bucket=self.bucket, endpoint=self.endpoint, directory=dir, credentials=credentials))
        r = scanned.map(objectstorage.Read(bucket=self.bucket, endpoint=self.endpoint, credentials=credentials))
        r.print()

        tester = Tester(topo)
        tester.run_for(60)
        tester.tuple_count(r, 2, exact=False)
        tester.test(self.test_ctxtype, self.test_config, always_collect_logs=True)

    @unittest.skipUnless(_cos_iam_env_var(), "COS_IAM_CREDENTIALS required")
    def test_app_config(self):
        if ("TestStreamingAnalytics" in str(self)):
            self.skipTest("Skip test, because there is no option to create app config from remote for Streaming Analytics service.")
        print ('\n---------'+str(self))
        name = 'test_app_config'
        topo = Topology(name)
        rnd=''.join(random.choice(string.digits) for _ in range(10))
        app_config = 'cos' + rnd
        print(app_config)

        creds_dict = self._get_credentials()
        creds_json = json.dumps(creds_dict)

        if (("TestDistributed" in str(self) or "TestICP" in str(self)) and _cp4d_url_env_var() is True):
            instance = streamsx.rest_primitives.Instance.of_endpoint(verify=False)
            res = objectstorage.configure_connection(instance, credentials=creds_dict, name=app_config)
            print (str(res))
        else: 
            if _streams_install_env_var():        
                _run_shell_command_line('streamtool rmappconfig --noprompt '+app_config)
                print ('create appConfig with streamtool: ' + app_config)
                creds_json = str(creds_json).replace('"','\\"')
                cmd = os.environ['STREAMS_INSTALL']+'/bin/streamtool mkappconfig --property cos.creds=\"'+creds_json+'\" '+app_config
                stdout, stderr, rc = _run_shell_command_line(cmd)
                print (str(rc))
            else:
                print ('Application configuration not created')

        credentials=app_config
        self._run_tester_app(name, '/sample'+rnd, credentials)

    @unittest.skipUnless(_cos_iam_env_var(), "COS_IAM_CREDENTIALS required")
    def test_basic_composite(self):
        print ('\n---------'+str(self))
        name = 'test_basic_composite'
        credentials=self._get_credentials()
        rnd=''.join(random.choice(string.digits) for _ in range(10))
        self._run_tester_app(name, '/sample'+rnd, credentials)

    @unittest.skipUnless(_cos_hmac_env_var(), "COS_HMAC_CREDENTIALS required")
    def test_credentials_hmac(self):
        print ('\n---------'+str(self))
        name = 'test_credentials_hmac'
        credentials=self._get_hmac_credentials()
        rnd=''.join(random.choice(string.digits) for _ in range(10))
        self._run_tester_app(name, '/sample'+rnd, credentials)

    @unittest.skipUnless(_cos_iam_env_var(), "COS_IAM_CREDENTIALS required")
    def test_parquet_composite(self):
        print ('\n---------'+str(self))
        name = 'test_parquet_composite'
        credentials=self._get_credentials()
        topo = Topology(name)
        if self.objectstorage_toolkit_home is not None:
            streamsx.spl.toolkit.add_toolkit(topo, self.objectstorage_toolkit_home)
        to_cos = topo.source(['Hello', 'World!'])
        to_cos = to_cos.as_string()
        to_cos.for_each(objectstorage.WriteParquet(self.bucket, self.endpoint, 'testc%OBJECTNUM.parquet', time_per_object=5, credentials=credentials))
        
        scanned_objects = topo.source(objectstorage.Scan(self.bucket, self.endpoint, 'testc0.parquet', credentials=credentials))
        scanned_objects.print()

        tester = Tester(topo)
        tester.run_for(60)
        tester.tuple_count(scanned_objects, 1, exact=False)
        tester.test(self.test_ctxtype, self.test_config, always_collect_logs=True)

    @unittest.skipUnless(_cos_iam_env_var(), "COS_IAM_CREDENTIALS required")
    def test_functions(self):
        print ('\n---------'+str(self))
        name = 'test_functions'
        credentials=self._get_credentials()

        topo = Topology(name)
        if self.objectstorage_toolkit_home is not None:
            streamsx.spl.toolkit.add_toolkit(topo, self.objectstorage_toolkit_home)
        to_cos = topo.source(['Hello', 'World!'])
        to_cos = to_cos.as_string()
        objectstorage.write(to_cos, self.bucket, self.endpoint, credentials=credentials, object='test%OBJECTNUM.time', time_per_object=datetime.timedelta(minutes=1), header="TEST_HEADER_ROW")
        objectstorage.write_parquet(to_cos, self.bucket, self.endpoint, 'test%OBJECTNUM.parquet', time_per_object=5, credentials=credentials)
        scanned_objects = objectstorage.scan(topo, bucket=self.bucket, endpoint=self.endpoint, pattern='test0.time', credentials=credentials)
        scanned_objects.print()
        r = objectstorage.read(scanned_objects, bucket=self.bucket, endpoint=self.endpoint, credentials=credentials)
        r.print()

        tester = Tester(topo)
        tester.run_for(60)
        tester.tuple_count(scanned_objects, 1, exact=False)
        tester.test(self.test_ctxtype, self.test_config, always_collect_logs=True)

    # Test with local MinIO endpoint with HTTP
    @unittest.skipUnless(_streams_install_env_var(), "STREAMS_INSTALL required")
    def test_credentials_minio(self):
        if _minio_credentials() == False:
            self.skipTest("Missing MINIO_HMAC_CREDENTIALS environment variable.")
        if ("TestDistributed" in str(self)):
            cred_file = os.environ['MINIO_HMAC_CREDENTIALS']
            credentials = None
            if os.path.exists(cred_file):
                print("MINIO HMAC credentials file: " + cred_file)
                with open(cred_file) as data_file:
                    credentials = json.load(data_file)
            else:
                print("MINIO HMAC credentials app config: " + cred_file)
                credentials = cred_file

            endpoint=os.environ["MINIO_ENDPOINT"]

            topo = Topology('ObjectStorageHelloWorldWithCredsMinio')
            if self.objectstorage_toolkit_home is not None:
                streamsx.spl.toolkit.add_toolkit(topo, self.objectstorage_toolkit_home)
            to_cos = topo.source(['Hello', 'World!'])
            to_cos = to_cos.as_string()
            objectstorage.write(to_cos, self.bucket, endpoint, '/minio/hw_%OBJECTNUM.txt', credentials=credentials, ssl_enabled=False)

            scanned = objectstorage.scan(topo, bucket=self.bucket, endpoint=endpoint, directory='/minio', credentials=credentials, ssl_enabled=False)
            r = objectstorage.read(scanned, bucket=self.bucket, endpoint=endpoint, credentials=credentials, ssl_enabled=False)
            r.print()
        
            tester = Tester(topo)
            tester.run_for(60)
            tester.tuple_count(r, 2, exact=True) # expect two lines 1:hello 2:World!
            tester.test(self.test_ctxtype, self.test_config, always_collect_logs=True)
        else:
            self.skipTest("TestDistributed is supported for tests with local minIO only")


class TestICPRemote(TestDistributed):

    @classmethod
    def setUpClass(self):
        super().setUpClass()

    def setUp(self):
        Tester.setup_distributed(self)
        self.objectstorage_toolkit_home = None
        self.test_config[context.ConfigParams.SSL_VERIFY] = False 

class TestStreamingAnalytics(TestDistributed):

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


