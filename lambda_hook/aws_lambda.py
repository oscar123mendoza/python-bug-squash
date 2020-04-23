import os
import os.path
import stat
import logging
import hashlib
from io import BytesIO
from zipfile import ZipFile, ZIP_DEFLATED
import botocore

import formic


"""Mask to retrieve only UNIX file permissions from the external attributes
field of a ZIP entry.
"""
ZIP_PERMS_MASK = (stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO) << 16

logger = logging.getLogger(__name__)


def _zip_files(files, root):
    """Generates a ZIP file in-memory from a list of files.

    Files will be stored in the archive with relative names, and have their
    UNIX permissions forced to 755 or 644 (depending on whether they are
    user-executable in the source filesystem).

    Args:
        files (list[str]): file names to add to the archive, relative to
            ``root``.
        root (str): base directory to retrieve files from.

    Returns:
        str: content of the ZIP file as a byte string.

    """
    zip_data = BytesIO()
    with ZipFile(zip_data, 'w', ZIP_DEFLATED) as zip_file:
        for fname in files:
            zip_file.write(os.path.join(root, fname), fname)

        # Fix file permissions to avoid any issues - only care whether a file
        # is executable or not, choosing between modes 755 and 644 accordingly.
        for zip_entry in zip_file.filelist:
            perms = (zip_entry.external_attr & ZIP_PERMS_MASK) >> 16
            if perms & stat.S_IXUSR != 0:
                new_perms = 0o755
            else:
                new_perms = 0o644

            if new_perms != perms:
                logger.debug("lambda: fixing perms: %s: %o => %o",
                             zip_entry.filename, perms, new_perms)
                new_attr = ((zip_entry.external_attr & ~ZIP_PERMS_MASK) |
                            (new_perms << 16))
                zip_entry.external_attr = new_attr

    contents = zip_data.getvalue()
    zip_data.close()

    return contents


def _find_files(root, includes, excludes):
    """List files inside a directory based on include and exclude rules.

    This is a more advanced version of `glob.glob`, that accepts multiple
    complex patterns.

    Args:
        root (str): base directory to list files from.
        includes (list[str]): inclusion patterns. Only files matching those
            patterns will be included in the result.
        includes (list[str]): exclusion patterns. Files matching those
            patterns will be excluded from the result. Exclusions take
            precedence over inclusions.

    Yields:
        str: a file name relative to the root.

    Note:
        Documentation for the patterns can be found at
        http://www.aviser.asia/formic/doc/index.html
    """

    root = os.path.abspath(root)
    file_set = formic.FileSet(directory=root, include=includes,
                              exclude=excludes)
    for filename in file_set.qualified_files(absolute=False):
        yield filename


def _zip_from_file_patterns(root, includes, excludes):
    """Generates a ZIP file in-memory from file search patterns.

    Args:
        root (str): base directory to list files from.
        includes (list[str]): inclusion patterns. Only files  matching those
            patterns will be included in the result.
        includes (list[str]): exclusion patterns. Files matching those
            patterns will be excluded from the result. Exclusions take
            precedence over inclusions.

    See Also:
        :func:`_zip_files`, :func:`_find_files`.

    Raises:
        RuntimeError: when the generated archive would be empty.

    """
    logger.info('lambda: base directory: %s', root)

    files = list(_find_files(root, includes, excludes))
    if not files:
        raise RuntimeError('Empty list of files for Lambda payload. Check '
                           'your include/exclude options for errors.')

    logger.info('lambda: adding %d files:', len(files))

    for fname in files:
        logger.debug('lambda: + %s', fname)

    return _zip_files(files, root)


def _head_object(s3_conn, bucket, key):
    """Retrieve information about an object in S3 if it exists.

    Args:
        s3_conn (:class:`botocore.client.S3`): S3 connection to use for
            operations.
        bucket (str): name of the bucket containing the key.
        key (str): name of the key to lookup.

    Returns:
        dict: S3 object information, or None if the object does not exist.
        See the AWS documentation for explanation of the contents.

    Raises:
        botocore.exceptions.ClientError: any error from boto3 other than key
            not found is passed through.
    """
    try:
        return s3_conn.head_object(Bucket=bucket, Key=key)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            return None
        else:
            raise


def _ensure_bucket(s3_conn, bucket):
    """Create an S3 bucket if it does not already exist.

    Args:
        s3_conn (:class:`botocore.client.S3`): S3 connection to use for
            operations.
        bucket (str): name of the bucket to create.

    Returns:
        dict: S3 object information. See the AWS documentation for explanation
        of the contents.

    Raises:
        botocore.exceptions.ClientError: any error from boto3 is passed
            through.
    """
    try:
        s3_conn.head_bucket(Bucket=bucket)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 404:
            logger.info('Creating bucket %s.', bucket)
            s3_conn.create_bucket(Bucket=bucket)
        elif e.response['Error']['Code'] in (401, 403):
            logger.exception('Access denied for bucket %s.', bucket)
            raise
        else:
            logger.exception('Error creating bucket %s. Error %s', bucket,
                             e.response)
            raise


def _upload_code(s3_conn, bucket_name, name, contents):
    """Upload a ZIP file to S3 for use by Lambda.

    The key used for the upload will be unique based on the checksum of the
    contents. No changes will be made if the contents in S3 already match the
    expected contents.

    Args:
        s3_conn (:class:`botocore.client.S3`): S3 connection to use for
            operations.
        bucket (str): name of the bucket to create.
        prefix (str): S3 prefix to prepend to the constructed key name for
            the uploaded file
        name (str): desired name of the Lambda function. Will be used to
            construct a key name for the uploaded file.
        contents (str): byte string with the content of the file upload.

    Returns:
        troposphere.awslambda.Code: CloudFormation Lambda Code object,
        pointing to the uploaded payload in S3.

    Raises:
        botocore.exceptions.ClientError: any error from boto3 is passed
            through.
    """

    hsh = hashlib.md5(contents)
    logger.debug('lambda: ZIP hash: %s', hsh.hexdigest())

    key = 'lambda-{}-{}.zip'.format(name, hsh.hexdigest())

    info = _head_object(s3_conn, bucket_name, key)
    expected_etag = '"{}"'.format(hsh.hexdigest())

    if info and info['ETag'] == expected_etag:
        logger.info('lambda: object %s already exists, not uploading', key)
    else:
        logger.info('lambda: uploading object %s', key)
        s3_conn.put_object(Bucket=bucket_name, Key=key, Body=contents,
                           ContentType='application/zip',
                           ACL='authenticated-read')

    return {"bucket": bucket_name, "key": key}


def _check_pattern_list(patterns, key, default=None):
    """Validates file search patterns from user configuration.

    Acceptable input is a string (which will be converted to a singleton list),
    a list of strings, or anything falsy (such as None or an empty dictionary).
    Empty or unset input will be converted to a default.

    Args:
        patterns: input from user configuration (YAML).
        key (str): name of the configuration key the input came from,
            used for error display purposes.

    Keyword Args:
        default: value to return in case the input is empty or unset.

    Returns:
        list[str]: validated list of patterns

    Raises:
        ValueError: if the input is unacceptable.
    """
    if not patterns:
        return default

    if isinstance(patterns, str):
        return [patterns]

    if isinstance(patterns, list):
        if all(isinstance(p, str) for p in patterns):
            return patterns

    raise ValueError("Invalid file patterns in key '{}': must be a string or "
                     'list of strings'.format(key))


def _upload_function(s3_conn, bucket_name, function_name, path,
                     include=None, exclude=None):
    """Builds a Lambda payload from user configuration and uploads it to S3.

    Args:
        s3_conn (:class:`botocore.client.S3`): S3 connection to use for
            operations.
        bucket_name (str): name of the bucket to upload to.
        function_name (str): desired name of the Lambda function. Will be used
            to construct a key name for the uploaded file.
        path (str): base path to retrieve files from (mandatory).
        include (list): file patterns to include in the payload (optional).
        exclude (list): file patterns to exclude from the payload (optional).

    Returns:
        dict: A dictionary with the bucket & key where the code is located.

    """
    root = os.path.expanduser(path)

    includes = _check_pattern_list(include, 'include', default=['**'])
    excludes = _check_pattern_list(exclude, 'exclude', default=[])

    logger.debug('lambda: processing function %s', function_name)

    zip_contents = _zip_from_file_patterns(root, includes, excludes)

    return _upload_code(s3_conn, bucket_name, function_name, zip_contents)


def upload_lambda_functions(s3_conn, bucket_name, function_name, path,
                            include=None, exclude=None):
    """Builds Lambda payloads from user configuration and uploads them to S3.

    Constructs ZIP archives containing files matching specified patterns for
    each function, uploads the result to Amazon S3, then returns the bucket
    and key name where the function is stored.

    Args:
        s3_conn (:class:`botocore.client.S3`): S3 connection to use for
            operations.
        bucket_name (str): name of the bucket to upload to.
        function_name (str): desired name of the Lambda function. Will be used
            to construct a key name for the uploaded file.
        path (str): base path to retrieve files from (mandatory).
        include (list): file patterns to include in the payload (optional).
        exclude (list): file patterns to exclude from the payload (optional).

    Returns:
        dict: A dictionary with the bucket & key where the code is located.

    """

    _ensure_bucket(s3_conn, bucket_name)

    results = _upload_function(s3_conn, bucket_name, function_name, path,
                               include, exclude)

    return results
