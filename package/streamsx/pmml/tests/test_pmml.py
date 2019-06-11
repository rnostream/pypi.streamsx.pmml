import streamsx.pmml as pmml

from streamsx.topology.topology import Topology
from streamsx.topology.tester import Tester
from streamsx.topology.schema import CommonSchema, StreamSchema
import streamsx.spl.op as op
import streamsx.spl.toolkit
import streamsx.rest as sr
import unittest
import datetime
import os
import json
from subprocess import call, Popen, PIPE

def cloud_creds_env_var():
    result = True
    try:
        os.environ['MACHINE_LEARNING_SERVICE_CREDENTIALS']
    except KeyError: 
        result = False
    return result

def pmml_model_file():
    script_dir = os.path.dirname(os.path.realpath(__file__))
    return script_dir+'/model.xml'

class Test(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        print (str(self))
        self.pmml_toolkit_home = os.environ["PMML_TOOLKIT_HOME"]
        
    def _build_only(self, name, topo):
        result = streamsx.topology.context.submit("TOOLKIT", topo.graph) # creates tk* directory
        print(name + ' (TOOLKIT):' + str(result))
        assert(result.return_code == 0)
        result = streamsx.topology.context.submit("BUNDLE", topo.graph)  # creates sab file
        print(name + ' (BUNDLE):' + str(result))
        assert(result.return_code == 0)

    def _get_credentials(self):
        if cloud_creds_env_var() == True:
            creds_file = os.environ['MACHINE_LEARNING_SERVICE_CREDENTIALS']
            with open(creds_file) as data_file:
                credentials = json.load(data_file)
        else:
            credentials = json.loads('{"username" : "user", "password" : "xxx", "url" : "xxx", "instance_id" : "xxx"}')
        return credentials

    def _create_stream(self, topo):
        s = topo.source([1,2,3,4,5,6])
        schema=StreamSchema('tuple<int32 id, rstring name>').as_tuple()
        return s.map(lambda x : (x,'X'+str(x*2)), schema=schema)

    def test_model_feed_bad_polling_time_param(self):
        print ('\n---------'+str(self))
        name = 'test_model_feed_bad_polling_time_param'
        topo = Topology(name)
        credentials = self._get_credentials()
        # expect TypeError because polling_period is wrong type (string)
        self.assertRaises(TypeError, pmml.model_feed, topo, connection_configuration=credentials, model_name="any_model", polling_period='1')
        # expect ValueError because polling_period is too small (< 1 sec)
        self.assertRaises(ValueError, pmml.model_feed, topo, connection_configuration=credentials, model_name="any_model", polling_period=datetime.timedelta(microseconds=50))

    def test_model_feed_bundle(self):
        print ('\n---------'+str(self))
        name = 'test_model_feed_bundle'
        topo = Topology(name)
        streamsx.spl.toolkit.add_toolkit(topo, self.pmml_toolkit_home)
        credentials = self._get_credentials()
        res = pmml.model_feed(topo, connection_configuration=credentials, model_name="any_model", polling_period=datetime.timedelta(minutes=5))
        res.print()
        if (("TestDistributed" in str(self)) or ("TestStreamingAnalytics" in str(self))):
            self._launch(topo)
        else:
            # build only
            self._build_only(name, topo)

    def test_score_bundle(self):
        print ('\n---------'+str(self))
        name = 'test_score_bundle'
        topo = Topology(name)
        streamsx.spl.toolkit.add_toolkit(topo, self.pmml_toolkit_home)
        s = self._create_stream(topo) # stream with two attributes id, name
        out_schema = StreamSchema('tuple<rstring success, rstring errorReason, rstring result>')
        res = pmml.score(s, schema=out_schema, model_input_attribute_mapping='p1=id,p2=name', model_path=pmml_model_file(), success_attribute_name='success', error_reason_attribute_name='errorReason', raw_result_attribute_name='result')
        res.print()
        if (("TestDistributed" in str(self)) or ("TestStreamingAnalytics" in str(self))):
            self._launch(topo)
        else:
            # build only
            self._build_only(name, topo)

    def test_score_bundle_string_input(self):
        print ('\n---------'+str(self))
        name = 'test_score_bundle_string_input'
        topo = Topology(name)
        streamsx.spl.toolkit.add_toolkit(topo, self.pmml_toolkit_home)
        s = topo.source(['first tuple', 'second tuple']).as_string()
        out_schema = StreamSchema('tuple<rstring string, rstring result>')
        res = pmml.score(s, schema=out_schema, model_input_attribute_mapping='p=string', model_path=pmml_model_file(), raw_result_attribute_name='result')
        res.print()
        if (("TestDistributed" in str(self)) or ("TestStreamingAnalytics" in str(self))):
            self._launch(topo)
        else:
            # build only
            self._build_only(name, topo)

    def test_score_bundle_output_mapping(self):
        print ('\n---------'+str(self))
        name = 'test_score_bundle_output_mapping'
        topo = Topology(name)
        streamsx.spl.toolkit.add_toolkit(topo, self.pmml_toolkit_home)
        s = topo.source(['first tuple', 'second tuple']).as_string()
        out_schema = StreamSchema('tuple<rstring string, rstring resultValue>')
        res = pmml.score(s, schema=out_schema, model_input_attribute_mapping='p=string', model_path=pmml_model_file(), model_output_attribute_mapping='resultValue=predictedValue')
        res.print()
        if (("TestDistributed" in str(self)) or ("TestStreamingAnalytics" in str(self))):
            self._launch(topo)
        else:
            # build only
            self._build_only(name, topo)

    def test_score_with_feed_on_second_input_port(self):
        print ('\n---------'+str(self))
        name = 'test_score_with_feed_on_second_input_port'
        topo = Topology(name)
        streamsx.spl.toolkit.add_toolkit(topo, self.pmml_toolkit_home)

        credentials = self._get_credentials()
        models = pmml.model_feed(topo, connection_configuration=credentials, model_name="sample_pmml", polling_period=datetime.timedelta(minutes=5))
        # sample with a single model predictor field
        s = topo.source(['first tuple', 'second tuple']).as_string()
        out_schema = StreamSchema('tuple<rstring string, rstring result>')
        res = pmml.score(s, schema=out_schema, model_input_attribute_mapping='p=string', model_stream=models, raw_result_attribute_name='result', initial_model_provisioning_timeout=datetime.timedelta(minutes=1))
        res.print()

        if (("TestDistributed" in str(self)) or ("TestStreamingAnalytics" in str(self))):
            self._launch(topo)
        else:
            # build only
            self._build_only(name, topo)

class TestDistributed(Test):
    def setUp(self):
        # setup test config
        self.test_config = {}
        job_config = streamsx.topology.context.JobConfig(tracing='info')
        job_config.add(self.test_config)
        self.test_config[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False  

    def _launch(self, topo):
        rc = streamsx.topology.context.submit('DISTRIBUTED', topo, self.test_config)
        print(str(rc))
        if rc is not None:
            if (rc.return_code == 0):
                rc.job.cancel()

class TestStreamingAnalytics(Test):
    def setUp(self):
        # setup test config
        self.test_config = {}
        job_config = streamsx.topology.context.JobConfig(tracing='info')
        job_config.add(self.test_config)

    def _launch(self, topo):
        rc = streamsx.topology.context.submit('STREAMING_ANALYTICS_SERVICE', topo, self.test_config)
        print(str(rc))
        if rc is not None:
            if (rc.return_code == 0):
                rc.job.cancel()

    @classmethod
    def setUpClass(self):
        # start streams service
        connection = sr.StreamingAnalyticsConnection()
        service = connection.get_streaming_analytics()
        result = service.start_instance()
        super().setUpClass()

