# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019

"""
Overview
++++++++

Provides functions to score input records using PMML models and to interact with the Watson Machine Learning (WML) repository.

This package is compatible with Streaming Analytics service on IBM Cloud:

  * `IBM Streaming Analytics <https://www.ibm.com/cloud/streaming-analytics>`_
  * `IBM Cloud Machine-Learning-Service <https://console.bluemix.net/catalog/services/machine-learning>`_



"""

__version__='0.1.0'

__all__ = ['score', 'model_feed']
from streamsx.pmml._pmml import score, model_feed

