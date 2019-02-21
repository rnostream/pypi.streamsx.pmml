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

def run_shell_command_line(command):
    process = Popen(command, universal_newlines=True, shell=True, stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()
    return stdout, stderr, process.returncode

def clean_up():
    run_shell_command_line('rm -rf tk*') 
    run_shell_command_line('rm -f *.sab *.json') 

def pmml_model_file():
    script_dir = os.path.dirname(os.path.realpath(__file__))
    return script_dir+'/model.xml'

class Test(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        print (str(self))
        self.pmml_toolkit_home = os.environ["PMML_TOOLKIT_HOME"]
        clean_up()
        
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

    def test_model_feed_bad_cred_param(self):
        print ('\n---------'+str(self))
        topo = Topology()
        # expect TypeError because credentials is not a dict
        self.assertRaises(TypeError, pmml.model_feed, topo, credentials='invalid', model_name="any_model")
        # expect ValueError because credentials is not expected JSON format
        invalid_creds = json.loads('{"user" : "user", "password" : "xx", "uri" : "xxx"}')
        self.assertRaises(ValueError, pmml.model_feed, topo, credentials=invalid_creds, model_name="any_model")

    def test_model_feed_bad_polling_time_param(self):
        print ('\n---------'+str(self))
        name = 'test_model_feed_bad_polling_time_param'
        topo = Topology(name)
        credentials = self._get_credentials()
        # expect TypeError because polling_period is wrong type (string)
        self.assertRaises(TypeError, pmml.model_feed, topo, credentials=credentials, model_name="any_model", polling_period='1')
        # expect ValueError because polling_period is too small (< 1 sec)
        self.assertRaises(ValueError, pmml.model_feed, topo, credentials=credentials, model_name="any_model", polling_period=datetime.timedelta(microseconds=50))

    def test_model_feed_bundle(self):
        print ('\n---------'+str(self))
        name = 'test_model_feed_bundle'
        topo = Topology(name)
        streamsx.spl.toolkit.add_toolkit(topo, self.pmml_toolkit_home)
        credentials = self._get_credentials()
        res = pmml.model_feed(topo, credentials=credentials, model_name="any_model", polling_period=datetime.timedelta(minutes=5))
        res.print()
        # build only
        self._build_only(name, topo)

    def test_score_bundle(self):
        print ('\n---------'+str(self))
        name = 'test_score_bundle'
        topo = Topology(name)
        streamsx.spl.toolkit.add_toolkit(topo, self.pmml_toolkit_home)
        s = self._create_stream(topo) # stream with two attributes id, name
        out_schema = StreamSchema('tuple<rstring success, rstring errorReason, rstring result>')
        res = pmml.score(s, schema=out_schema, input_model_fields='field1, field2', model_path=pmml_model_file(), input_stream_attribute_names='id, name', success_attribute_name='success', error_reason_attribute_name='errorReason', raw_result_attribute_name='result')
        res.print()
        # build only
        self._build_only(name, topo)

    def test_score_bundle_string_input(self):
        print ('\n---------'+str(self))
        name = 'test_score_bundle_string_input'
        topo = Topology(name)
        streamsx.spl.toolkit.add_toolkit(topo, self.pmml_toolkit_home)
        s = topo.source(['first tuple', 'second tuple']).as_string()
        out_schema = StreamSchema('tuple<rstring string, rstring result>')
        res = pmml.score(s, schema=out_schema, input_model_fields='field1', model_path=pmml_model_file(), raw_result_attribute_name='result')
        res.print()
        # build only
        self._build_only(name, topo)

    def test_score_with_feed_on_second_input_port(self):
        print ('\n---------'+str(self))
        name = 'test_score_with_feed_on_second_input_port'
        topo = Topology(name)
        streamsx.spl.toolkit.add_toolkit(topo, self.pmml_toolkit_home)

        credentials = self._get_credentials()
        models = pmml.model_feed(topo, credentials=credentials, model_name="sample_pmml", polling_period=datetime.timedelta(minutes=5))
        # sample with a single model predictor field
        s = topo.source(['first tuple', 'second tuple']).as_string()
        out_schema = StreamSchema('tuple<rstring string, rstring result>')
        res = pmml.score(s, schema=out_schema, input_model_fields='sample_field', model_stream=models, raw_result_attribute_name='result', initial_model_provisioning_timeout=datetime.timedelta(minutes=1))
        res.print()

        # build only
        self._build_only(name, topo)


#class TestDistributed(Test):
#    def setUp(self):
#        Tester.setup_distributed(self)

#class TestStreamingAnalytics(Test):
#    def setUp(self):
#        Tester.setup_streaming_analytics(self, force_remote_build=True)

#    @classmethod
#    def setUpClass(self):
#        # start streams service
#        connection = sr.StreamingAnalyticsConnection()
#        service = connection.get_streaming_analytics()
#        result = service.start_instance()

