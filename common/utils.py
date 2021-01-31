import boto3

from .singleton import Singleton
from .customLogger import CustomLogger

logger = CustomLogger.get_logger(__name__)


class Utils(object, metaclass=Singleton):
    @classmethod
    def upload_file_s3(cls, file_name, bucket, object_name):
        """Upload a file to an S3 bucket

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """
        logger.info("{0}.upload_file_s3(): Inside".format(cls.__name__))

        # Upload the file
        s3_client = boto3.client('s3')
        try:
            logger.info("{0}.upload_file_s3(): File name to upload = {1}".format(cls.__name__, file_name))
            s3_client.upload_file(file_name, bucket, object_name)
        except Exception as e:
            logger.error("{0}.upload_file_s3(): Error: {1}".format(cls.__name__, e))
            return False

        logger.info("{0}.upload_file_s3(): Out".format(cls.__name__))
        return True
