import datetime as dt
import io
import os
import socket
import sys
import threading
import time

import boto3
import cachetools
import pandas as pd
from botocore.exceptions import ClientError
from pgraws import pgraws

CACHE_DURATION = dt.timedelta(seconds=3300)
CACHE_SIZE = 10

auth_cache = cachetools.TTLCache(CACHE_SIZE, CACHE_DURATION, dt.datetime.now)
role_cache = cachetools.TTLCache(CACHE_SIZE, CACHE_DURATION, dt.datetime.now)
auth_lock = threading.Lock()
role_lock = threading.Lock()


class AwsHelper:
    def __init__(self):
        self.aws_session_with_role = None
        self.aws_session_without_role = None

    @staticmethod
    def reset_credentials():
        """reset credentials if expired token"""
        os.system("unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN")
        time.sleep(5)  # race condition for writing AWS credentials
        os.system("pgraws -r D-U-AWS27-CLAIMS-DATASCIENTISTS -c")
        time.sleep(5)  # race condition for writing AWS credentials

    def create_session(self, try_again=True, assume_role: bool = True):
        """create a boto3 session with good credentials

        Args:
            try_again: bool, boolean value to denote whether to try this function again
            assume_role: whether to assume the cross account role in session or not
        """
        if not socket.gethostname().startswith("ip-"):
            self.reset_credentials()
        try:
            self.aws_session_without_role = boto3.Session()
        except ClientError:
            if try_again:
                self.reset_credentials()
                self.create_session(try_again=False, assume_role=assume_role)
            else:
                self.aws_session_with_role = "failed at ClientError"
        except ConnectionRefusedError:
            print("caught ConnectionRefusedError")
            time.sleep(10)
            if try_again:
                self.create_session(try_again=False, assume_role=assume_role)
            else:
                self.aws_session_with_role = "failed at ConnectionRefusedError"
        except Exception:
            print(sys.exc_info()[0])
            if try_again:
                self.create_session(try_again=False, assume_role=assume_role)
            else:
                self.aws_session_with_role = "failed at general except"


@cachetools.cached(auth_cache, cachetools.keys.hashkey, auth_lock)
def _authenticate(role: str) -> dict:
    return pgraws.get_saml_credentials(role)


def _format_creds(creds_response: dict, boto_format: bool):
    creds = {
        "aws_access_key_id"
        if boto_format
        else "ACCESS_KEY": (creds_response["AccessKeyId"]),
        "aws_secret_access_key"
        if boto_format
        else "SECRET_ACCESS_KEY": (creds_response["SecretAccessKey"]),
        "aws_session_token"
        if boto_format
        else "SESSION_TOKEN": (creds_response["SessionToken"]),
    }
    return creds


@cachetools.cached(role_cache, cachetools.keys.hashkey, role_lock)
def _get_role_arn_creds(base_role: str, cross_role_arn: str, boto_format: bool) -> dict:
    response = _authenticate(base_role)
    auth_creds = _format_creds(response, True)

    sts_client = boto3.client("sts", **auth_creds)
    response = sts_client.assume_role(
        RoleArn=cross_role_arn,
        RoleSessionName="document-indexing-prefect",
        DurationSeconds=3600,
    )
    role_creds = _format_creds(response["Credentials"], boto_format)
    return role_creds


def get_creds(role: str, boto_format: bool = False) -> dict:
    response = _authenticate(role)
    creds = _format_creds(response, boto_format)
    return creds


def get_cross_role_creds(
    base_role: str, cross_role: tuple, boto_format: bool = False
) -> dict:
    cross_role_arn = f"arn:aws:iam::{cross_role[0]}:role/{cross_role[1]}"
    role_creds = _get_role_arn_creds(base_role, cross_role_arn, boto_format)
    return role_creds


def read_s3(bucket_name, file_path) -> pd.DataFrame:
    """bring data from AWS S3 bucket to local memory, after establishing a session"""
    aws_sess = AwsHelper()
    aws_sess.create_session(assume_role=False)
    session = aws_sess.aws_session_without_role
    s3 = session.resource("s3")

    data = s3.Object(bucket_name, file_path).get()["Body"].read().decode("utf-8")

    if file_path.endswith(".csv"):
        return pd.read_csv(io.StringIO(data))
    elif file_path.endswith(".parquet"):
        return pd.read_parquet(io.StringIO(data))
    else:
        return pd.DataFrame()
