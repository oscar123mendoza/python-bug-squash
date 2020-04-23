# Python bug squash

This exercise represents an actual bug found in an open source project. The project sometimes uploads the contents of an AWS Lambda to S3.

The way it does this is to zip up all the contents of a given directory then builds a hash of the contents of that zip file, appending the hash to the filename.  (ie: `lambda-<functionName>-<hash>.zip`).  It then checks to see if a file of the same name (and therefor, the same hash) already exists in S3. If it does, it is not re-uploaded. if that filename does not exist in s3, then it uploads the new file.

The content hash generated by the AWS lambda hook seems to change periodically, and is different across machines even when nothing else has changed. The expected behaviour of the hook is for the generated payload to be uploaded only when the contents of the zip file has changed. So if the hook is run on two separate machines for the same configuration file the resulting payload should only be uploaded once - unfortunately, that is not the case.

Please investigate why this is breaking (we've provided a test-suite that shows the breakage) and come up with a fix (which should allow the tests to run successfully).

Note: You should not need to change the existing tests to make this work, though you can add new tests if you want to test additional functionality you add.

## Running the tests

### Locally

First make sure you have pip & virtualenv installed.  If not, then perform the following:

```
easy_install pip
pip install --upgrade pip
pip install virtualenv
```

Now instantiate a virtualenv and activate it:

```
virtualenv chime-bug-squash
. ./chime-bug-squash/bin/activate
```

Now install all the necessary dependencies:

```
pip install -r requirements.txt
```

You should now be able to run the test suite from the top level directory of the repo:

```
# Note: This will break at first, due to the bug. After your fix, it should run
# successfully.
nosetests
```

### Docker

Run this command which will build the docker image, and run a bash shell inside it:
```
make shell
```

Then directly in the docker container:
```
$ nosetests
```
