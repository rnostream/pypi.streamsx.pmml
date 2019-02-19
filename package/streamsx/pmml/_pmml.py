# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019

import os
import streamsx.spl.op
import streamsx.spl.types
from streamsx.topology.schema import CommonSchema, StreamSchema
from streamsx.spl.types import rstring
import datetime

def _read_ml_service_credentials(credentials):
    username = ""
    password = ""
    instance_id = ""
    url = ""
    if isinstance(credentials, dict):
        username = credentials.get('username')
        password = credentials.get('password')
        instance_id = credentials.get('instance_id')
        url = credentials.get('url')
    else:
        raise TypeError(credentials)
    return username, password, instance_id, url

def _check_time_param(time_value, parameter_name):
    if isinstance(time_value, datetime.timedelta):
        result = time_value.total_seconds()
    elif isinstance(time_value, int):
        result = time_value
    else:
        raise TypeError(time_value)
    if result <= 1:
        raise ValueError("Invalid "+parameter_name+" value. Value must be at least one second.")
    return result

def score(stream, schema, name=None):
    """Uses the PMMLScoring operator to score tuple data.

    The PMMLScoring operator scores tuple data it receives on the first port, mapping input attributes to model predictors of a configurable PMML model, which may be updated via a second port during runtime. The predicted value (score) is sent together with the original input tuple and some model meta information to the ouput port.
    The model data can be loaded from a file on startup of the operator. Additionally, model data can be sent to the second input port in PMML format. This allows to update the model during runtime. 

    Args:
        stream(Stream): Stream of tuples containing the records to be scored.
        schema(Schema): Output streams schema 
        name(str): Operator name in the Streams context, defaults to a generated name.

    Returns:
        Output Stream with specified schema
    """

    _op = _PMMLScoring(stream, schema=schema, name=name)

    return _op.outputs[0]


def model_feed(topology, credentials, model_name=None, model_uid=None, polling_period=None, name=None):
    """Downloads a Machine Learning (ML) model from the `IBM Cloud Machine-Learning-Service <https://console.bluemix.net/catalog/services/machine-learning>`_ as input for PMML ``score`` function.

    Models can be created and trained in Watson Studio or by using notebooks.

    Args:
        topology(Topology): Topology to contain the returned stream.
        credentials(dict): The credentials of the IBM cloud Machine Learning service in *JSON*.
        model_name(str): A model in the WML repository can be referenced by its name or UID. When you use the name, keep in mind that in the concept of the WML repository the name is ambiguous. Different models may have the same name. The only unique identifier is the model UID. Using the name may be more comfortable as the UID is a long digit string. When you are using the name, make sure that the name is unique in the WML repository. If a name is not unique, the operator will use the first model that matches the name. Use either the ``model_name`` parameter or the ``model_uid`` parameter, if both are given model_name is ignored. If none of them is given the application will not successfully start up. 
        model_uid(str): In the WML repository a models UID is a unique identifier. If the model is updated with a new version the UID is the still the same. Use either ``model_name`` or ``model_uid`` parameter, if both are given ``model_name`` is ignored. If none of them is given the application will not successfully start up. 
        polling_period(int|datetime.timedelta): The ``polling_period`` controls the interval between the calls to the WML repository. Value can be specified in seconds if 'int' type is used or in 'datetime.timedelta' format.
        name(str): Source name in the Streams context, defaults to a generated name.

    Returns:
        Stream: Object names stream with schema ``com.ibm.streams.pmml::ModelData``.
    """

    # check if model_uid or model_name is set
    if (model_uid is None and model_name is None):
        raise ValueError("Use either model_name or model_uid parameter.")

    username, password, instance_id, url = _read_ml_service_credentials(credentials)

    _op = _WMLModelFeed(topology, schema='com.ibm.streams.pmml::ModelData', userName=username, userPassword=password, wmlInstanceId=instance_id, wmlUrl=url, name=name)
    if polling_period is not None:
        _op.params['pollingPeriod'] = streamsx.spl.types.int32(_check_time_param(polling_period, 'polling_period'))
    if model_uid is not None:
        _op.params['modelUid'] = model_uid
    if model_name is not None:
        _op.params['modelName'] = model_name
    return _op.outputs[0]


class _WMLModelFeed(streamsx.spl.op.Source):
    def __init__(self, topology, schema, userName=None, userPassword=None, wmlInstanceId=None, wmlUrl=None, modelName=None, modelUid=None, pollingPeriod=None, name=None):
        kind="com.ibm.streams.pmml::WMLModelFeed"
        inputs=None
        schemas=schema
        params = dict()
        # Required: userName, userPassword, wmlInstanceId, wmlUrl     
        params['userName'] = userName
        params['userPassword'] = userPassword
        params['wmlInstanceId'] = wmlInstanceId
        params['wmlUrl'] = wmlUrl
        # Optional: modelName, modelUid, pollingPeriod
        if modelName is not None:
            params['modelName'] = modelName
        if modelUid is not None:
            params['modelUid'] = modelUid
        if pollingPeriod is not None:
            params['pollingPeriod'] = pollingPeriod

        super(_WMLModelFeed, self).__init__(topology,kind,schemas,params,name)


class _PMMLScoring(streamsx.spl.op.Invoke):
    def __init__(self, stream, schema, attributesMappingMode=None, errorReasonAttributeName=None, initialModelProvisioningTimeout=None, inputModelFields=None, inputStreamAttributeNames=None, inputStreamAttributes=None, modelOutputAttributeMapping=None, modelPath=None, rawResultAttributeName=None, successAttributeName=None, wmlMetaDataAttributeName=None, name=None):
        topology = stream.topology
        kind="com.ibm.streams.pmml::PMMLScoring"
        inputs=stream
        schemas=schema
        params = dict()

        if attributesMappingMode is not None:
            params['attributesMappingMode'] = attributesMappingMode
        if errorReasonAttributeName is not None:
            params['errorReasonAttributeName'] = errorReasonAttributeName
        if initialModelProvisioningTimeout is not None:
            params['initialModelProvisioningTimeout'] = initialModelProvisioningTimeout
        if inputModelFields is not None:
            params['inputModelFields'] = inputModelFields
        if inputStreamAttributeNames is not None:
            params['inputStreamAttributeNames'] = inputStreamAttributeNames
        if inputStreamAttributes is not None:
            params['inputStreamAttributes'] = inputStreamAttributes
        if modelOutputAttributeMapping is not None:
            params['modelOutputAttributeMapping'] = modelOutputAttributeMapping
        if modelPath is not None:
            params['modelPath'] = modelPath
        if rawResultAttributeName is not None:
            params['rawResultAttributeName'] = rawResultAttributeName
        if successAttributeName is not None:
            params['successAttributeName'] = successAttributeName
        if wmlMetaDataAttributeName is not None:
            params['wmlMetaDataAttributeName'] = wmlMetaDataAttributeName
 
        super(_PMMLScoring, self).__init__(topology,kind,inputs,schema,params,name)

