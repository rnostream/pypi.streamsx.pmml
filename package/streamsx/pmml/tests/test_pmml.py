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

def cloud_creds_env_var():
    result = True
    try:
        os.environ['MACHINE_LEARNING_SERVICE_CREDENTIALS']
    except KeyError: 
        result = False
    return result


class Test(unittest.TestCase):

    @unittest.skipIf(cloud_creds_env_var() == False, "Missing MACHINE_LEARNING_SERVICE_CREDENTIALS environment variable.")
    def test_model_feed(self):
        topo = Topology('test_model_feed')
        streamsx.spl.toolkit.add_toolkit(topo, self.pmml_toolkit_home)
        creds_file = os.environ['MACHINE_LEARNING_SERVICE_CREDENTIALS']
        with open(creds_file) as data_file:
            credentials = json.load(data_file)
        res = pmml.model_feed(topo, credentials=credentials, model_name="any_model", polling_period=datetime.timedelta(minutes=5))
        res.print()

        tester = Tester(topo)
        tester.tuple_count(res, 1)
        #tester.run_for(60)
        # setup test config
        cfg = {}
        job_config = streamsx.topology.context.JobConfig(tracing='info')
        job_config.add(cfg)
        cfg[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False     
        # Run the test
        tester.test(self.test_ctxtype, cfg, always_collect_logs=True)

    def test_score(self):
        topo = Topology('test_score')
        streamsx.spl.toolkit.add_toolkit(topo, self.pmml_toolkit_home)

        s = topo.source(['first tuple', 'second tuple']).as_string()
        out_schema = StreamSchema('tuple<rstring A, rstring B>')
        res = pmml.score(s, out_schema)
        res.print()

        tester = Tester(topo)
        tester.tuple_count(res, 2)
        #tester.run_for(60)
        # setup test config
        cfg = {}
        job_config = streamsx.topology.context.JobConfig(tracing='info')
        job_config.add(cfg)
        cfg[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False     
        # Run the test
        tester.test(self.test_ctxtype, cfg, always_collect_logs=True)

class TestDistributed(Test):
    def setUp(self):
        Tester.setup_distributed(self)
        self.pmml_toolkit_home = os.environ["PMML_TOOLKIT_HOME"]

class TestStreamingAnalytics(Test):
    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)
        self.pmml_toolkit_home = os.environ["PMML_TOOLKIT_HOME"]

    @classmethod
    def setUpClass(self):
        # start streams service
        connection = sr.StreamingAnalyticsConnection()
        service = connection.get_streaming_analytics()
        result = service.start_instance()

