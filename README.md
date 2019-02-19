# Python streamsx.pmml package

This exposes SPL operators in the `com.ibm.streams.pmml` toolkit as Python methods.

Package is organized using standard packaging to upload to PyPi.

The package is uploaded to PyPi in the standard way:
```
cd package
python setup.py sdist bdist_wheel upload -r pypi
```
Note: This is done using the `ibmstreams` account at pypi.org and requires `.pypirc` file containing the credentials in your home directory.

Package details: https://pypi.python.org/pypi/streamsx.pmml

Documentation is using Sphinx and can be built locally using:
```
cd package/docs
make html
```
and viewed using
```
firefox package/docs/build/html/index.html
```

The documentation is also setup at `readthedocs.io`.

Documentation links:
* http://streamsxpmml.readthedocs.io

## Version update

To change the version information of the Python package, edit following files:

- ./package/docs/source/conf.py
- ./package/streamsx/pmml/\_\_init\_\_.py

When the development status changes, edit the *classifiers* in

- ./package/setup.py

When the documented sample must be changed, change it here:

- ./package/streamsx/pmml/\_\_init\_\_.py
- ./package/DESC.txt

## Test

Package can be tested with TopologyTester using the [Streaming Analytics](https://www.ibm.com/cloud/streaming-analytics) service.

```
cd package
python3 -u -m unittest streamsx.pmml.tests.test_pmml.TestStreamingAnalytics
```

Package can be tested with TopologyTester using a local and running Streams domain.
Make sure that the streams environment is set, the domain and instance is running and the environment variables:
STREAMS_USERNAME
STREAMS_PASSWORD
are setup.

```
cd package
python3 -u -m unittest streamsx.pmml.tests.test_pmml.TestDistributed
```
