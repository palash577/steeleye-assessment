import xmltodict
import urllib.request
import zipfile
import xml.etree.ElementTree as ET
import csv

from common.customLogger import CustomLogger
from common.utils import Utils

logger = CustomLogger.get_logger(__name__)


def main():
    url = ("https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&"
           "fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100")
    download_xml_file(url)


def download_xml_file(url):
    """
    Download xml file from given url
    :param url:
    """
    logger.info("{0}: Inside".format(download_xml_file.__name__))
    try:
        data_arr = None
        with urllib.request.urlopen(url) as res:
            data = res.read()
            data = xmltodict.parse(data)
            data_arr = data['response']['result']['doc']

        if data_arr:
            url_zip = None
            flag = False
            for x in data_arr:
                for y in x['str']:
                    if y['@name'] == 'download_link':
                        url_zip = y['#text']
                    if y['@name'] == 'file_type' and y['#text'] == 'DLTINS':
                        flag = True
                        break
                if flag:
                    break
            logger.info("{0}: {1}".format(download_xml_file.__name__, url_zip))
            zip_file_name = url_zip.split("/")[-1]
            zip_file_path = "./{0}".format(zip_file_name)
            urllib.request.urlretrieve(url_zip, zip_file_path)
            logger.info("zip_file_path: {0}".format(zip_file_path))
            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                zip_ref.extractall("./")

            zip_file_path = 'DLTINS_20210117_01of01.zip'
            xml_file_path = "{0}.xml".format(zip_file_path[:-4])
            xml_to_csv(xml_file_path)

    except Exception as e:
        logger.error(e)
    finally:
        logger.info("{0}: Out".format(download_xml_file.__name__))


def xml_to_csv(file_path):
    """
    Converts the xml file to custom csv
    :param file_path: Location of the file to convert
    """
    logger.info("{0}: Inside".format(xml_to_csv.__name__))
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()

        count = 0
        csv_header = ["Id", "FullNm", "ClssfctnTp", "CmmdtyDerivInd", "NtnlCcy", "Issr"]
        with open('{0}.csv'.format(file_path[:-4]), mode='w') as d_file:
            d_writer = csv.writer(d_file, quoting=csv.QUOTE_NONNUMERIC)
            d_writer.writerow(csv_header)
            w_row = []
            for child in root.iter():
                if count == 1:
                    tag_name = child.tag.split('}')[1]
                    if tag_name in csv_header and len(w_row) < len(csv_header):
                        w_row.insert(csv_header.index(tag_name), child.text)
                if 'FinInstrmGnlAttrbts' in child.tag:
                    if count == 1:
                        d_writer.writerow(w_row)
                        w_row = []
                        count = 0
                    else:
                        count = 1

        s3_bucket = ''
        s3_object_name = ''
        Utils.upload_file_s3('{0}.csv'.format(file_path[:-4]), s3_bucket, s3_object_name)
    except Exception as e:
        logger.error(e)
    finally:
        logger.info("{0}: Out".format(xml_to_csv.__name__))


main()
