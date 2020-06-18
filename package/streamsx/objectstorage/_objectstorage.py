# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2018

import datetime

import streamsx.spl.op
import streamsx.spl.types
from streamsx.topology.schema import CommonSchema, StreamSchema
from streamsx.spl.types import rstring
import json
from streamsx.toolkits import download_toolkit
import streamsx.topology.composite

_TOOLKIT_NAME = 'com.ibm.streamsx.objectstorage'

def _add_toolkit_dependency(topo, version):
    # IMPORTANT: Dependency of this python wrapper to a specific toolkit version
    # This is important when toolkit is not set with streamsx.spl.toolkit.add_toolkit (selecting toolkit from remote build service)
    streamsx.spl.toolkit.add_toolkit_dependency(topo, 'com.ibm.streamsx.objectstorage', version)


def configure_connection(instance, name='cos', credentials=None):
    """Configures IBM Streams for a certain connection.


    Creates an application configuration object containing the required properties with connection information.


    Example for creating a configuration for a Streams instance with connection details::

        from icpd_core import icpd_util
        from streamsx.rest_primitives import Instance
        import streamsx.objectstorage as cos

        cfg = icpd_util.get_service_instance_details(name='your-streams-instance', instance_type='streams')
        cfg[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False
        instance = Instance.of_service(cfg)
        app_cfg = cos.configure_connection(instance, credentials='my_credentials_json')


    Args:
        instance(streamsx.rest_primitives.Instance): IBM Streams instance object.
        name(str): Name of the application configuration, default name is 'cos'.
        credentials(str|dict): The service credentials for IBM Cloud Object Storage.
    Returns:
        Name of the application configuration.

    .. warning:: The function can be used only in IBM Cloud Pak for Data
    .. versionadded:: 1.1
    """

    description = 'COS credentials'
    properties = {}
    if credentials is None:
        raise TypeError(credentials)

    if isinstance(credentials, dict):
        properties['cos.creds'] = json.dumps(credentials)
    else:
        properties['cos.creds'] = credentials

    # check if application configuration exists
    app_config = instance.get_application_configurations(name=name)
    if app_config:
        print('update application configuration: ' + name)
        app_config[0].update(properties)
    else:
        print('create application configuration: ' + name)
        instance.create_application_configuration(name, properties, description)
    return name


def download_toolkit(url=None, target_dir=None):
    r"""Downloads the latest Objectstorage toolkit from GitHub.

    Example for updating the Objectstorage toolkit for your topology with the latest toolkit from GitHub::

        import streamsx.objectstorage as objectstorage
        # download toolkit from GitHub
        objectstorage_toolkit_location = objectstorage.download_toolkit()
        # add the toolkit to topology
        streamsx.spl.toolkit.add_toolkit(topology, objectstorage_toolkit_location)

    Example for updating the topology with a specific version of the Objectstorage toolkit using a URL::

        import streamsx.objectstorage as objectstorage
        url1100 = 'https://github.com/IBMStreams/streamsx.objectstorage/releases/download/v1.10.0/streamsx.objectstorage.toolkits-1.10.0-20190730-1132.tgz'
        objectstorage_toolkit_location = objectstorage.download_toolkit(url=url1100)
        streamsx.spl.toolkit.add_toolkit(topology, objectstorage_toolkit_location)

    Args:
        url(str): Link to toolkit archive (\*.tgz) to be downloaded. Use this parameter to 
            download a specific version of the toolkit.
        target_dir(str): the directory where the toolkit is unpacked to. If a relative path is given,
            the path is appended to the system temporary directory, for example to /tmp on Unix/Linux systems.
            If target_dir is ``None`` a location relative to the system temporary directory is chosen.

    Returns:
        str: the location of the downloaded Objectstorage toolkit

    .. note:: This function requires an outgoing Internet connection
    .. versionadded:: 1.3
    """
    _toolkit_location = streamsx.toolkits.download_toolkit (toolkit_name=_TOOLKIT_NAME, url=url, target_dir=target_dir)
    return _toolkit_location


def _read_hmac_credentials(credentials):
    access_key_id = None
    secret_access_key = None
    access_key_id = credentials.get('access_key_id')
    secret_access_key = credentials.get('secret_access_key')
    return access_key_id, secret_access_key


def _check_time_per_object(time_per_object):
    if isinstance(time_per_object, datetime.timedelta):
        result = time_per_object.total_seconds()
    elif isinstance(time_per_object, int) or isinstance(time_per_object, float):
        result = time_per_object
    else:
        raise TypeError(time_per_object)
    if result <= 1:
        raise ValueError("Invalid time_per_object value. Value must be at least one second.")
    return result


class Scan(streamsx.topology.composite.Source):
    """Scan a directory in a bucket for object names.

    Scans an object storage directory and emits the names of new or modified objects that are found in the directory.

    Example scanning a directory ``/sample`` for objects matching the pattern::

        import streamsx.objectstorage as cos

        scans = topo.source(cos.Scan(bucket='your-bucket-name', directory='/sample', pattern='SAMPLE_[0-9]*\\.ascii\\.text$'))

    .. versionadded:: 1.5

    Attributes
    ----------
    bucket : str
        Bucket name. Bucket must have been created in your Cloud Object Storage service before using this class.
    endpoint : str
        Endpoint for Cloud Object Storage. Select the endpoint for your bucket location and resiliency: `IBM® Cloud Object Storage Endpoints <https://console.bluemix.net/docs/services/cloud-object-storage/basics/endpoints.html>`_. Use a private enpoint when running in IBM cloud Streaming Analytics service.
    pattern : str
        Limits the object names that are listed to the names that match the specified regular expression.
    directory : str
        Specifies the name of the directory to be scanned. Any subdirectories are not scanned.
    credentials : str|dict
        Credentials as dict or name of the application configuration containing the credentials for Cloud Object Storage. When set to ``None`` the application configuration ``cos`` is used.
    protocol: str
        Protocol used by the S3 client, either ``cos`` (IAM and HMAC authentication supported) or  ``s3a`` (requires HMAC authentication). Protocol ``s3a`` supports multipart upload. `Protocol selection <https://ibmstreams.github.io/streamsx.objectstorage/doc/spldoc/html/tk$com.ibm.streamsx.objectstorage/tk$com.ibm.streamsx.objectstorage$19.html>`_
    options : kwargs
        The additional optional parameters as variable keyword arguments.

    Returns:
        Stream: Object names stream with schema ``CommonSchema.String``.
    """

    def __init__(self, bucket, endpoint, pattern='.*', directory='/', credentials=None, protocol='cos', **options):
        self.bucket = bucket
        self.endpoint = endpoint
        self.pattern = pattern
        self.directory = directory
        self.credentials = credentials
        if (protocol != 'cos' and protocol != 's3a'):
            raise ValueError("Set 'cos' or 's3a' for the protocol parameter.")
        else:
            self.protocol = protocol

        self.ssl_enabled = None
        self.vm_arg = None
        if 'ssl_enabled' in options:
            self.ssl_enabled = options.get('ssl_enabled')
        if 'vm_arg' in options:
            self.vm_arg = options.get('vm_arg')

    @property
    def vm_arg(self):
        """
            str: Arbitrary JVM arguments can be passed. For example, increase JVM's maximum heap size ``'-Xmx 8192m'``.
        """
        return self._vm_arg

    @vm_arg.setter
    def vm_arg(self, value):
        self._vm_arg = value
        

    @property
    def ssl_enabled(self):
        """
            bool: Set to ``False`` if you want to use HTTP instead of HTTPS. Per default SSL is enabled and HTTPS is used.
        """
        return self._ssl_enabled

    @ssl_enabled.setter
    def ssl_enabled(self, value):
        self._ssl_enabled = value

    def populate(self, topology, name, **options):
        app_config_name = self.credentials
        # check if it's the credentials for the service
        if isinstance(self.credentials, dict):
            app_config_name = None

        _op = _ObjectStorageScan(topology, CommonSchema.String, pattern = self.pattern, directory = self.directory, endpoint = self.endpoint, appConfigName = app_config_name, vmArg = self.vm_arg, name = name)
        _op.params['objectStorageURI'] = self.protocol+'://'+self.bucket

        if isinstance(self.credentials, dict):
            access_key_id, secret_access_key = _read_hmac_credentials(self.credentials)
            if access_key_id is not None and secret_access_key is not None:
                _op.params['objectStorageUser'] = access_key_id
                _op.params['objectStoragePassword'] = secret_access_key
            else:
                _op.params['credentials'] = json.dumps(self.credentials)

        if self.ssl_enabled is not None:
            if self.ssl_enabled is False:
                _add_toolkit_dependency(topology, '[1.10.0,3.0.0)')
                _op.params['sslEnabled'] =  _op.expression('false')

        return _op.outputs[0]


class Read(streamsx.topology.composite.Map):
    """Read an object in a bucket.

    Reads the object specified in the input stream and emits content of the object. Expects ``CommonSchema.String`` in the input stream.

    Example of reading object with the objects names from the ``scanned`` stream::

        import streamsx.objectstorage as cos

        r = scanned.map(cos.Read(bucket=bucket, endpoint=endpoint))

    .. versionadded:: 1.5

    Attributes
    ----------
    bucket : str
        Bucket name. Bucket must have been created in your Cloud Object Storage service before using this class.
    endpoint : str
        Endpoint for Cloud Object Storage. Select the endpoint for your bucket location and resiliency: `IBM® Cloud Object Storage Endpoints <https://console.bluemix.net/docs/services/cloud-object-storage/basics/endpoints.html>`_. Use a private enpoint when running in IBM cloud Streaming Analytics service.
    credentials : str|dict
        Credentials as dict or name of the application configuration containing the credentials for Cloud Object Storage. When set to ``None`` the application configuration ``cos`` is used.
    protocol: str
        Protocol used by the S3 client, either ``cos`` (IAM and HMAC authentication supported) or  ``s3a`` (requires HMAC authentication). Protocol ``s3a`` supports multipart upload. `Protocol selection <https://ibmstreams.github.io/streamsx.objectstorage/doc/spldoc/html/tk$com.ibm.streamsx.objectstorage/tk$com.ibm.streamsx.objectstorage$19.html>`_
    options : kwargs
        The additional optional parameters as variable keyword arguments.

    Returns:
        :py:class:`topology_ref:streamsx.topology.topology.Stream`: Object content line by line with schema ``CommonSchema.String``.
    """
    def __init__(self, bucket, endpoint, credentials=None, protocol='cos', **options):
        self.bucket = bucket
        self.endpoint = endpoint
        self.credentials = credentials
        if (protocol != 'cos' and protocol != 's3a'):
            raise ValueError("Set 'cos' or 's3a' for the protocol parameter.")
        else:
            self.protocol = protocol

        self.ssl_enabled = None
        self.vm_arg = None
        if 'ssl_enabled' in options:
            self.ssl_enabled = options.get('ssl_enabled')
        if 'vm_arg' in options:
            self.vm_arg = options.get('vm_arg')

    @property
    def vm_arg(self):
        """
            str: Arbitrary JVM arguments can be passed. For example, increase JVM's maximum heap size ``'-Xmx 8192m'``.
        """
        return self._vm_arg

    @vm_arg.setter
    def vm_arg(self, value):
        self._vm_arg = value

    @property
    def ssl_enabled(self):
        """
            bool: Set to ``False`` if you want to use HTTP instead of HTTPS. Per default SSL is enabled and HTTPS is used.
        """
        return self._ssl_enabled

    @ssl_enabled.setter
    def ssl_enabled(self, value):
        self._ssl_enabled = value


    def populate(self, topology, stream, schema, name, **options):
        app_config_name = self.credentials
        # check if it's the credentials for the service
        if isinstance(self.credentials, dict):
            app_config_name = None

        _op = _ObjectStorageSource(stream, CommonSchema.String, endpoint = self.endpoint, appConfigName = app_config_name, vmArg = self.vm_arg, name = name)
        _op.params['objectStorageURI'] = self.protocol+'://'+self.bucket

        if isinstance(self.credentials, dict):
            access_key_id, secret_access_key = _read_hmac_credentials(self.credentials)
            if access_key_id is not None and secret_access_key is not None:
                _op.params['objectStorageUser'] = access_key_id
                _op.params['objectStoragePassword'] = secret_access_key
            else:
                _op.params['credentials'] = json.dumps(self.credentials)

        if self.ssl_enabled is not None:
            if self.ssl_enabled is False:
                _add_toolkit_dependency(topology, '[1.10.0,3.0.0)')
                _op.params['sslEnabled'] =  _op.expression('false')

        return _op.outputs[0]


class Write(streamsx.topology.composite.ForEach):
    """Write strings to an object.

    Adds a COS-Writer where each tuple on `stream` is
    written into an object.

    Expects ``CommonSchema.String`` in the input stream.

    Example of creating an object with two lines::

        import streamsx.objectstorage as cos
        to_cos = topo.source(['Hello', 'World!'])
        to_cos = to_cos.as_string()
        to_cos.for_each(cos.Write(bucket, endpoint, '/sample/hw%OBJECTNUM.txt'))

    .. versionadded:: 1.5

    Attributes
    ----------
    bucket : str
        Bucket name. Bucket must have been created in your Cloud Object Storage service before using this class.
    endpoint : str
        Endpoint for Cloud Object Storage. Select the endpoint for your bucket location and resiliency: `IBM® Cloud Object Storage Endpoints <https://console.bluemix.net/docs/services/cloud-object-storage/basics/endpoints.html>`_. Use a private enpoint when running in IBM cloud Streaming Analytics service.
    object : str
        Name of the object to be created in your bucket. For example, ``SAMPLE_%OBJECTNUM.text``, %OBJECTNUM is an object number, starting at 0. When a new object is opened for writing the number is incremented.
    time_per_object : int|float|datetime.timedelta
        Specifies the approximate time, in seconds, after which the current output object is closed and a new object is opened for writing.
    credentials : str|dict
        Credentials as dict or name of the application configuration containing the credentials for Cloud Object Storage. When set to ``None`` the application configuration ``cos`` is used.
    protocol: str
        Protocol used by the S3 client, either ``cos`` (IAM and HMAC authentication supported) or  ``s3a`` (requires HMAC authentication). Protocol ``s3a`` supports multipart upload. `Protocol selection <https://ibmstreams.github.io/streamsx.objectstorage/doc/spldoc/html/tk$com.ibm.streamsx.objectstorage/tk$com.ibm.streamsx.objectstorage$19.html>`_
    options : kwargs
        The additional optional parameters as variable keyword arguments.

    Returns:
        :py:class:`topology_ref:streamsx.topology.topology.Sink`: Stream termination.
    """
    def __init__(self, bucket, endpoint, object, time_per_object=10.0, credentials=None, protocol='cos', **options):
        self.bucket = bucket
        self.endpoint = endpoint
        self.object = object
        self.credentials = credentials
        if (protocol != 'cos' and protocol != 's3a'):
            raise ValueError("Set 'cos' or 's3a' for the protocol parameter.")
        else:
            self.protocol = protocol
        self.time_per_object = time_per_object
        self.header = None
        self.ssl_enabled = None
        self.vm_arg = None
        if 'header' in options:
            self.header = options.get('header')
        if 'ssl_enabled' in options:
            self.ssl_enabled = options.get('ssl_enabled')
        if 'vm_arg' in options:
            self.vm_arg = options.get('vm_arg')

    @property
    def header(self):
        """
            str: Specify the content of the header row. This header is added as first line in the object. Use this parameter when writing strings in CSV format and you like to query the objects with the IBM SQL Query service. By default no header row is generated.
        """
        return self._header

    @header.setter
    def header(self, value):
        self._header = value

    @property
    def vm_arg(self):
        """
            str: Arbitrary JVM arguments can be passed. For example, increase JVM's maximum heap size ``'-Xmx 8192m'``.
        """
        return self._vm_arg

    @vm_arg.setter
    def vm_arg(self, value):
        self._vm_arg = value

    @property
    def ssl_enabled(self):
        """
            bool: Set to ``False`` if you want to use HTTP instead of HTTPS. Per default SSL is enabled and HTTPS is used.
        """
        return self._ssl_enabled

    @ssl_enabled.setter
    def ssl_enabled(self, value):
        self._ssl_enabled = value

    def populate(self, topology, stream, name, **options) -> streamsx.topology.topology.Sink:
        app_config_name = self.credentials
        # check if it's the credentials for the service
        if isinstance(self.credentials, dict):
            app_config_name = None

        _op = _ObjectStorageSink(stream, objectName = self.object, endpoint = self.endpoint, appConfigName = app_config_name, vmArg = self.vm_arg, name = name)
        _op.params['storageFormat'] = 'raw'
        _op.params['objectStorageURI'] = self.protocol+'://'+self.bucket
        _op.params['timePerObject'] = streamsx.spl.types.float64(_check_time_per_object(self.time_per_object))

        if self.header is not None:
            _op.params['headerRow'] = self.header

        if isinstance(self.credentials, dict):
            access_key_id, secret_access_key = _read_hmac_credentials(self.credentials)
            if access_key_id is not None and secret_access_key is not None:
                _op.params['objectStorageUser'] = access_key_id
                _op.params['objectStoragePassword'] = secret_access_key
            else:
                _op.params['credentials'] = json.dumps(self.credentials)

        if self.ssl_enabled is not None:
            if self.ssl_enabled is False:
                _add_toolkit_dependency(topology, '[1.10.0,3.0.0)')
                _op.params['sslEnabled'] =  _op.expression('false')

        return streamsx.topology.topology.Sink(_op)


class WriteParquet(streamsx.topology.composite.ForEach):
    """Create objects in parquet format.

    Adds a COS-Writer where each tuple on `stream` is
    written into an object in parquet format.

    Example of creating objects in parquet format from a stream named 'js' in JSON format::

        import streamsx.objectstorage as cos
        ...
        # JSON to tuple
        to_cos = js.map(schema='tuple<rstring a, int32 b>')
        to_cos.for_each(cos.write(bucket=bucket, endpoint=endpoint, object='/parquet/sample/hw%OBJECTNUM.parquet'))

    .. versionadded:: 1.5

    Attributes
    ----------
    bucket : str
        Bucket name. Bucket must have been created in your Cloud Object Storage service before using this class.
    endpoint : str
        Endpoint for Cloud Object Storage. Select the endpoint for your bucket location and resiliency: `IBM® Cloud Object Storage Endpoints <https://console.bluemix.net/docs/services/cloud-object-storage/basics/endpoints.html>`_. Use a private enpoint when running in IBM cloud Streaming Analytics service.
    object : str
        Name of the object to be created in your bucket. For example, ``SAMPLE_%OBJECTNUM.text``, %OBJECTNUM is an object number, starting at 0. When a new object is opened for writing the number is incremented.
    time_per_object : int|float|datetime.timedelta
        Specifies the approximate time, in seconds, after which the current output object is closed and a new object is opened for writing.
    credentials : str|dict
        Credentials as dict or name of the application configuration containing the credentials for Cloud Object Storage. When set to ``None`` the application configuration ``cos`` is used.
    protocol: str
        Protocol used by the S3 client, either ``cos`` (IAM and HMAC authentication supported) or  ``s3a`` (requires HMAC authentication). Protocol ``s3a`` supports multipart upload. `Protocol selection <https://ibmstreams.github.io/streamsx.objectstorage/doc/spldoc/html/tk$com.ibm.streamsx.objectstorage/tk$com.ibm.streamsx.objectstorage$19.html>`_
    options : kwargs
        The additional optional parameters as variable keyword arguments.

    Returns:
        :py:class:`topology_ref:streamsx.topology.topology.Sink`: Stream termination.
    """
    def __init__(self, bucket, endpoint, object, time_per_object=10.0, credentials=None, protocol='cos', **options):
        self.bucket = bucket
        self.endpoint = endpoint
        self.object = object
        self.credentials = credentials
        if (protocol != 'cos' and protocol != 's3a'):
            raise ValueError("Set 'cos' or 's3a' for the protocol parameter.")
        else:
            self.protocol = protocol
        self.time_per_object = time_per_object
        self.ssl_enabled = None
        self.vm_arg = None
        if 'header' in options:
            self.header = options.get('header')
        if 'ssl_enabled' in options:
            self.ssl_enabled = options.get('ssl_enabled')
        if 'vm_arg' in options:
            self.vm_arg = options.get('vm_arg')

    @property
    def vm_arg(self):
        """
            str: Arbitrary JVM arguments can be passed. For example, increase JVM's maximum heap size ``'-Xmx 8192m'``.
        """
        return self._vm_arg

    @vm_arg.setter
    def vm_arg(self, value):
        self._vm_arg = value

    @property
    def ssl_enabled(self):
        """
            bool: Set to ``False`` if you want to use HTTP instead of HTTPS. Per default SSL is enabled and HTTPS is used.
        """
        return self._ssl_enabled

    @ssl_enabled.setter
    def ssl_enabled(self, value):
        self._ssl_enabled = value

    def populate(self, topology, stream, name, **options) -> streamsx.topology.topology.Sink:
        app_config_name = self.credentials
        # check if it's the credentials for the service
        if isinstance(self.credentials, dict):
            app_config_name = None

        _op = _ObjectStorageSink(stream, objectName = self.object, endpoint = self.endpoint, appConfigName = app_config_name, vmArg = self.vm_arg, name = name)
        _op.params['storageFormat'] = 'parquet'
        _op.params['parquetCompression'] = 'SNAPPY'
        _op.params['parquetEnableDict'] = _op.expression('true')
        _op.params['objectStorageURI'] = self.protocol+'://'+self.bucket
        _op.params['timePerObject'] = streamsx.spl.types.float64(_check_time_per_object(self.time_per_object))

        if isinstance(self.credentials, dict):
            access_key_id, secret_access_key = _read_hmac_credentials(self.credentials)
            if access_key_id is not None and secret_access_key is not None:
                _op.params['objectStorageUser'] = access_key_id
                _op.params['objectStoragePassword'] = secret_access_key
            else:
                _op.params['credentials'] = json.dumps(self.credentials)

        if self.ssl_enabled is not None:
            if self.ssl_enabled is False:
                _add_toolkit_dependency(topology, '[1.10.0,3.0.0)')
                _op.params['sslEnabled'] =  _op.expression('false')

        return streamsx.topology.topology.Sink(_op)


def scan(topology, bucket, endpoint, pattern='.*', directory='/', credentials=None, ssl_enabled=None, vm_arg=None, name=None):
    """Scan a directory in a bucket for object names.

    Scans an object storage directory and emits the names of new or modified objects that are found in the directory.

    Example scanning a directory ``/sample`` for objects matching the pattern::

        import streamsx.objectstorage as cos

        scans = cos.scan(topo, bucket='your-bucket-name', directory='/sample', pattern='SAMPLE_[0-9]*\\.ascii\\.text$')

    Args:
        topology(Topology): Topology to contain the returned stream.
        bucket(str): Bucket name. Bucket must have been created in your Cloud Object Storage service before using this function.
        endpoint(str): Endpoint for Cloud Object Storage. Select the endpoint for your bucket location and resiliency: `IBM® Cloud Object Storage Endpoints <https://console.bluemix.net/docs/services/cloud-object-storage/basics/endpoints.html>`_. Use a private enpoint when running in IBM cloud Streaming Analytics service.
        pattern(str): Limits the object names that are listed to the names that match the specified regular expression.
        directory(str): Specifies the name of the directory to be scanned. Any subdirectories are not scanned.
        credentials(str|dict): Credentials as dict or name of the application configuration containing the credentials for Cloud Object Storage. When set to ``None`` the application configuration ``cos`` is used.
        ssl_enabled(bool): Set to ``False`` if you want to use HTTP instead of HTTPS. Per default SSL is enabled and HTTPS is used.
        vm_arg(str): Arbitrary JVM arguments can be passed. For example, increase JVM's maximum heap size ``'-Xmx 8192m'``.     
        name(str): Sink name in the Streams context, defaults to a generated name.

    Returns:
        Stream: Object names stream with schema ``CommonSchema.String``.


    .. deprecated:: 1.5.0
        Use the :py:class:`~Scan`.
    """

    appConfigName=credentials
    # check if it's the credentials for the service
    if isinstance(credentials, dict):
         appConfigName = None

    _op = _ObjectStorageScan(topology, CommonSchema.String, pattern=pattern, directory=directory, endpoint=endpoint, appConfigName=appConfigName, vmArg=vm_arg, name=name)
    _op.params['objectStorageURI'] = 'cos://'+bucket

    if isinstance(credentials, dict):
        access_key_id, secret_access_key = _read_hmac_credentials(credentials)
        if access_key_id is not None and secret_access_key is not None:
            _op.params['objectStorageUser'] = access_key_id
            _op.params['objectStoragePassword'] = secret_access_key
        else:
            _op.params['credentials'] = json.dumps(credentials)

    if ssl_enabled is not None:
        if ssl_enabled is False:
            _add_toolkit_dependency(topology, '[1.10.0,3.0.0)')
            _op.params['sslEnabled'] =  _op.expression('false')

    return _op.outputs[0]


def read(stream, bucket, endpoint, credentials=None, ssl_enabled=None, vm_arg=None, name=None):
    """Read an object in a bucket.

    Reads the object specified in the input stream and emits content of the object.

    Example of reading object with the objects names from the ``scanned`` stream::

        import streamsx.objectstorage as cos

        r = cos.read(scanned, bucket=bucket, endpoint=endpoint)

    Args:
        stream(streamsx.topology.topology.Stream): Stream of tuples with object names to be read. Expects ``CommonSchema.String`` in the input stream.
        bucket(str): Bucket name. Bucket must have been created in your Cloud Object Storage service before using this function.
        endpoint(str): Endpoint for Cloud Object Storage. Select the endpoint for your bucket location and resiliency: `IBM® Cloud Object Storage Endpoints <https://console.bluemix.net/docs/services/cloud-object-storage/basics/endpoints.html>`_. Use a private enpoint when running in IBM cloud Streaming Analytics service.
        credentials(str|dict): Credentials as dict or name of the application configuration containing the credentials for Cloud Object Storage. When set to ``None`` the application configuration ``cos`` is used.
        ssl_enabled(bool): Set to ``False`` if you want to use HTTP instead of HTTPS. Per default SSL is enabled and HTTPS is used.
        vm_arg(str): Arbitrary JVM arguments can be passed. For example, increase JVM's maximum heap size ``'-Xmx 8192m'``.        
        name(str): Sink name in the Streams context, defaults to a generated name.

    Returns:
        :py:class:`topology_ref:streamsx.topology.topology.Stream`: Object content line by line with schema ``CommonSchema.String``.


    .. deprecated:: 1.5.0
        Use the :py:class:`~Read`.
    """

    appConfigName=credentials
    # check if it's the credentials for the service
    if isinstance(credentials, dict):
         appConfigName = None

    _op = _ObjectStorageSource(stream, CommonSchema.String, endpoint=endpoint, appConfigName=appConfigName, vmArg=vm_arg, name=name)
    _op.params['objectStorageURI'] = 'cos://'+bucket

    if isinstance(credentials, dict):
        access_key_id, secret_access_key = _read_hmac_credentials(credentials)
        if access_key_id is not None and secret_access_key is not None:
            _op.params['objectStorageUser'] = access_key_id
            _op.params['objectStoragePassword'] = secret_access_key
        else:
            _op.params['credentials'] = json.dumps(credentials)

    if ssl_enabled is not None:
        if ssl_enabled is False:
            _add_toolkit_dependency(stream.topology, '[1.10.0,3.0.0)')
            _op.params['sslEnabled'] =  _op.expression('false')

    return _op.outputs[0]


def write(stream, bucket, endpoint, object, time_per_object=10.0, header=None, credentials=None, ssl_enabled=None, vm_arg=None, name=None):
    """Write strings to an object.

    Adds a COS-Writer where each tuple on `stream` is
    written into an object.

    Example of creating an object with two lines::

        import streamsx.objectstorage as cos
        to_cos = topo.source(['Hello', 'World!'])
        to_cos = to_cos.as_string()
        cos.write(to_cos, bucket, endpoint, '/sample/hw%OBJECTNUM.txt')

    Args:
        stream(streamsx.topology.topology.Stream): Stream of tuples to be written to an object. Expects ``CommonSchema.String`` in the input stream.
        bucket(str): Bucket name. Bucket must have been created in your Cloud Object Storage service before using this function.
        endpoint(str): Endpoint for Cloud Object Storage. Select the endpoint for your bucket location and resiliency: `IBM® Cloud Object Storage Endpoints <https://console.bluemix.net/docs/services/cloud-object-storage/basics/endpoints.html>`_. Use a private enpoint when running in IBM cloud Streaming Analytics service.
        object(str): Name of the object to be created in your bucket. For example, ``SAMPLE_%OBJECTNUM.text``, %OBJECTNUM is an object number, starting at 0. When a new object is opened for writing the number is incremented.
        time_per_object(int|float|datetime.timedelta): Specifies the approximate time, in seconds, after which the current output object is closed and a new object is opened for writing.
        header(str): Specify the content of the header row. This header is added as first line in the object. Use this parameter when writing strings in CSV format and you like to query the objects with the IBM SQL Query service. By default no header row is generated.
        credentials(str|dict): Credentials as dict or name of the application configuration containing the credentials for Cloud Object Storage. When set to ``None`` the application configuration ``cos`` is used.
        ssl_enabled(bool): Set to ``False`` if you want to use HTTP instead of HTTPS. Per default SSL is enabled and HTTPS is used.
        vm_arg(str): Arbitrary JVM arguments can be passed. For example, increase JVM's maximum heap size ``'-Xmx 8192m'``.
        name(str): Sink name in the Streams context, defaults to a generated name.

    Returns:
        :py:class:`topology_ref:streamsx.topology.topology.Sink`: Stream termination.


    .. deprecated:: 1.5.0
        Use the :py:class:`~Write`.
    """

    appConfigName=credentials
    # check if it's the credentials for the service
    if isinstance(credentials, dict):
         appConfigName = None

    _op = _ObjectStorageSink(stream, objectName=object, endpoint=endpoint, appConfigName=appConfigName, vmArg=vm_arg, name=name)
    _op.params['storageFormat'] = 'raw'
    _op.params['objectStorageURI'] = 'cos://'+bucket
    _op.params['timePerObject'] = streamsx.spl.types.float64(_check_time_per_object(time_per_object))

    if header is not None:
        _op.params['headerRow'] = header

    if isinstance(credentials, dict):
        access_key_id, secret_access_key = _read_hmac_credentials(credentials)
        if access_key_id is not None and secret_access_key is not None:
            _op.params['objectStorageUser'] = access_key_id
            _op.params['objectStoragePassword'] = secret_access_key
        else:
            _op.params['credentials'] = json.dumps(credentials)

    if ssl_enabled is not None:
        if ssl_enabled is False:
            _add_toolkit_dependency(stream.topology, '[1.10.0,3.0.0)')
            _op.params['sslEnabled'] =  _op.expression('false')

    return streamsx.topology.topology.Sink(_op)
    

def write_parquet(stream, bucket, endpoint, object, time_per_object=10.0, credentials=None, ssl_enabled=None, vm_arg=None, name=None):
    """Create objects in parquet format.

    Adds a COS-Writer where each tuple on `stream` is
    written into an object in parquet format.

    Example of creating objects in parquet format from a stream named 'js' in JSON format::

        import streamsx.objectstorage as cos
        ...
        # JSON to tuple
        to_cos = js.map(schema='tuple<rstring a, int32 b>')
        cos.write(to_cos, bucket=bucket, endpoint=endpoint, object='/parquet/sample/hw%OBJECTNUM.parquet')

    Args:
        stream(streamsx.topology.topology.Stream): Stream of tuples to be written to an object. Supports ``streamsx.topology.schema.StreamSchema`` (schema for a structured stream) as input. Attributes are mapped to parquet columns.
        bucket(str): Bucket name. Bucket must have been created in your Cloud Object Storage service before using this function.
        endpoint(str): Endpoint for Cloud Object Storage. Select the endpoint for your bucket location and resiliency: `IBM® Cloud Object Storage Endpoints <https://console.bluemix.net/docs/services/cloud-object-storage/basics/endpoints.html>`_. Use a private enpoint when running in IBM cloud Streaming Analytics service.
        object(str): Name of the object to be created in your bucket. For example, ``SAMPLE_%OBJECTNUM.parquet``, %OBJECTNUM is an object number, starting at 0. When a new object is opened for writing the number is incremented.
        time_per_object(int|float|datetime.timedelta): Specifies the approximate time, in seconds, after which the current output object is closed and a new object is opened for writing.
        credentials(str|dict): Credentials as dict or name of the application configuration containing the credentials for Cloud Object Storage. When set to ``None`` the application configuration ``cos`` is used.
        ssl_enabled(bool): Set to ``False`` if you want to use HTTP instead of HTTPS. Per default SSL is enabled and HTTPS is used.
        vm_arg(str): Arbitrary JVM arguments can be passed. For example, increase JVM's maximum heap size ``'-Xmx 8192m'``.
        name(str): Sink name in the Streams context, defaults to a generated name.

    Returns:
        :py:class:`topology_ref:streamsx.topology.topology.Sink`: Stream termination.


    .. deprecated:: 1.5.0
        Use the :py:class:`~WriteParquet`.
    """

    appConfigName=credentials
    # check if it's the credentials for the service
    if isinstance(credentials, dict):
         appConfigName = None

    _op = _ObjectStorageSink(stream, objectName=object, endpoint=endpoint, appConfigName=appConfigName, vmArg=vm_arg, name=name)
    _op.params['storageFormat'] = 'parquet'
    _op.params['parquetCompression'] = 'SNAPPY'
    _op.params['parquetEnableDict'] = _op.expression('true')
    _op.params['objectStorageURI'] = 'cos://'+bucket
    _op.params['timePerObject'] = streamsx.spl.types.float64(_check_time_per_object(time_per_object))
    if isinstance(credentials, dict):
        access_key_id, secret_access_key = _read_hmac_credentials(credentials)
        if access_key_id is not None and secret_access_key is not None:
            _op.params['objectStorageUser'] = access_key_id
            _op.params['objectStoragePassword'] = secret_access_key
        else:
            _op.params['credentials'] = json.dumps(credentials)

    if ssl_enabled is not None:
        if ssl_enabled is False:
            _add_toolkit_dependency(stream.topology, '[1.10.0,3.0.0)')
            _op.params['sslEnabled'] =  _op.expression('false')


    return streamsx.topology.topology.Sink(_op)


class _ObjectStorageSink(streamsx.spl.op.Invoke):
    def __init__(self, stream, schema=None, vmArg=None, appConfigName=None, bytesPerObject=None, closeOnPunct=None, dataAttribute=None, encoding=None, endpoint=None, headerRow=None, objectName=None, objectNameAttribute=None, objectStorageURI=None, parquetBlockSize=None, parquetCompression=None, parquetDictPageSize=None, parquetEnableDict=None, parquetEnableSchemaValidation=None, parquetPageSize=None, parquetWriterVersion=None, partitionValueAttributes=None, skipPartitionAttributes=None, storageFormat=None, timeFormat=None, timePerObject=None, tuplesPerObject=None, IAMApiKey=None, IAMServiceInstanceId=None, objectStorageUser=None, objectStoragePassword=None, sslEnabled=None, name=None):
        topology = stream.topology
        kind="com.ibm.streamsx.objectstorage::ObjectStorageSink"
        inputs=stream
        schemas=schema
        params = dict()
        if vmArg is not None:
            params['vmArg'] = vmArg
        if appConfigName is not None:
            params['appConfigName'] = appConfigName
        if bytesPerObject is not None:
            params['bytesPerObject'] = bytesPerObject
        if closeOnPunct is not None:
            params['closeOnPunct'] = closeOnPunct
        if dataAttribute is not None:
            params['dataAttribute'] = dataAttribute
        if encoding is not None:
            params['encoding'] = encoding
        if endpoint is not None:
            params['endpoint'] = endpoint
        if headerRow is not None:
            params['headerRow'] = headerRow
        if objectName is not None:
            params['objectName'] = objectName
        if objectNameAttribute is not None:
            params['objectNameAttribute'] = objectNameAttribute
        if objectStorageURI is not None:
            params['objectStorageURI'] = objectStorageURI
        if parquetBlockSize is not None:
            params['parquetBlockSize'] = parquetBlockSize
        if parquetCompression is not None:
            params['parquetCompression'] = parquetCompression
        if parquetDictPageSize is not None:
            params['parquetDictPageSize'] = parquetDictPageSize
        if parquetEnableDict is not None:
            params['parquetEnableDict'] = parquetEnableDict
        if parquetEnableSchemaValidation is not None:
            params['parquetEnableSchemaValidation'] = parquetEnableSchemaValidation
        if parquetPageSize is not None:
            params['parquetPageSize'] = parquetPageSize
        if parquetWriterVersion is not None:
            params['parquetWriterVersion'] = parquetWriterVersion
        if partitionValueAttributes is not None:
            params['partitionValueAttributes'] = partitionValueAttributes
        if skipPartitionAttributes is not None:
            params['skipPartitionAttributes'] = skipPartitionAttributes
        if storageFormat is not None:
            params['storageFormat'] = storageFormat
        if timeFormat is not None:
            params['timeFormat'] = timeFormat
        if timePerObject is not None:
            params['timePerObject'] = timePerObject
        if tuplesPerObject is not None:
            params['tuplesPerObject'] = tuplesPerObject
        if IAMApiKey is not None:
            params['IAMApiKey'] = IAMApiKey
        if IAMServiceInstanceId is not None:
            params['IAMServiceInstanceId'] = IAMServiceInstanceId
        if objectStorageUser is not None:
            params['objectStorageUser'] = objectStorageUser
        if objectStoragePassword is not None:
            params['objectStoragePassword'] = objectStoragePassword
        if sslEnabled is not None:
            params['sslEnabled'] = sslEnabled

        super(_ObjectStorageSink, self).__init__(topology,kind,inputs,schema,params,name)


class _ObjectStorageScan(streamsx.spl.op.Source):
    def __init__(self, topology, schema, directory, pattern, vmArg=None, appConfigName=None, endpoint=None, objectStorageURI=None, initDelay=None, sleepTime=None, strictMode=None, IAMApiKey=None, IAMServiceInstanceId=None, objectStorageUser=None, objectStoragePassword=None, sslEnabled=None, name=None):
        kind="com.ibm.streamsx.objectstorage::ObjectStorageScan"
        inputs=None
        schemas=schema
        params = dict()
        params['directory'] = directory
        if vmArg is not None:
            params['vmArg'] = vmArg
        if appConfigName is not None:
            params['appConfigName'] = appConfigName
        if endpoint is not None:
            params['endpoint'] = endpoint
        if objectStorageURI is not None:
            params['objectStorageURI'] = objectStorageURI
        if initDelay is not None:
            params['initDelay'] = initDelay
        if sleepTime is not None:
            params['sleepTime'] = sleepTime
        if pattern is not None:
            params['pattern'] = pattern
        if strictMode is not None:
            params['strictMode'] = strictMode
        if IAMApiKey is not None:
            params['IAMApiKey'] = IAMApiKey
        if IAMServiceInstanceId is not None:
            params['IAMServiceInstanceId'] = IAMServiceInstanceId
        if objectStorageUser is not None:
            params['objectStorageUser'] = objectStorageUser
        if objectStoragePassword is not None:
            params['objectStoragePassword'] = objectStoragePassword
        if sslEnabled is not None:
            params['sslEnabled'] = sslEnabled

        super(_ObjectStorageScan, self).__init__(topology,kind,schemas,params,name)


class _ObjectStorageSource(streamsx.spl.op.Invoke):
    
    def __init__(self, stream, schema, vmArg=None, appConfigName=None, endpoint=None, objectStorageURI=None, blockSize=None, encoding=None, initDelay=None, IAMApiKey=None, IAMServiceInstanceId=None, objectStorageUser=None, objectStoragePassword=None, sslEnabled=None, name=None):
        kind="com.ibm.streamsx.objectstorage::ObjectStorageSource"
        topology = stream.topology
        inputs=stream
        params = dict()
        if vmArg is not None:
            params['vmArg'] = vmArg
        if appConfigName is not None:
            params['appConfigName'] = appConfigName
        if endpoint is not None:
            params['endpoint'] = endpoint
        if objectStorageURI is not None:
            params['objectStorageURI'] = objectStorageURI
        if blockSize is not None:
            params['blockSize'] = initDelay
        if encoding is not None:
            params['encoding'] = sleepTime
        if initDelay is not None:
            params['initDelay'] = initDelay
        if IAMApiKey is not None:
            params['IAMApiKey'] = IAMApiKey
        if IAMServiceInstanceId is not None:
            params['IAMServiceInstanceId'] = IAMServiceInstanceId
        if objectStorageUser is not None:
            params['objectStorageUser'] = objectStorageUser
        if objectStoragePassword is not None:
            params['objectStoragePassword'] = objectStoragePassword
        if sslEnabled is not None:
            params['sslEnabled'] = sslEnabled

        super(_ObjectStorageSource, self).__init__(topology,kind,inputs,schema,params,name)


