# =============================================================================
# Filename:     ftp-delta-neutral.py
# Author:       Peter Kim
# Description:  Script on AWS Cloud9 to check DeltaNeutral FTP site.
#               DeltaNeutral has 30 days of zip files of option prices.
#               Compare with zip and unzip files in S3 bucket.
#               For files not in S3, download from FTP, upload to S3.
#               Unzip the files and store in new S3 folder.
#
# =============================================================================
#
# References:
#  (1) https://github.com/orasik/aws_lambda_ftp_function
#  (2) https://github.com/orasik/aws_lambda_ftp_function/blob/master/aws_lambda_ftp_function.py
#  (3) https://medium.com/@johnpaulhayes/how-extract-a-huge-zip-file-in-an-amazon-s3-bucket-by-using-aws-lambda-and-python-e32c6cf58f06
#  (4) https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html
#  (5) https://stackoverflow.com/questions/39272397/uploading-file-to-specific-folder-in-s3-using-boto3
#
# =============================================================================


from __future__ import print_function
import boto3
import os
import ftplib
import time

import json

# for retrieving credentials from AWS Secrets Manager
import boto3
import base64
from botocore.exceptions import ClientError

import zipfile
from io import BytesIO


def get_secret(str_secret_name):
    '''
    Retrieve DeltaNeutral FTP credentials from AWS Secrets Manager.
    Returns a json string which needs to be converted to dictionary.
    
    Input:
        str_secret_name - string name of secret in AWS Secrets Manager.  
    '''

    secret_name = str_secret_name
    region_name = "us-west-2"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    
    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        
        return get_secret_value_response['SecretString']
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
        

def get_s3_file_names(in_bucket, in_folder=None):
    '''
    Get list of files in S3 bucket and folder, already downloaded previously.  
    
    Using the boto3.client() for S3.
    Other alternative is to install the s3fs Python library.  
    
    Optionally provide folder name within the bucket.
    
    Input:
        in_bucket - str. name of AWS S3 bucket
        in_folder - str, optional.  name of folder within S3 bucket.
    Return:
        list of files in AWS S3 bucket.
    '''
    
    # instantiate boto3-S3 client
    s3_client = boto3.client('s3')

    # get objects in bucket, response is dictionary with lots of metadata
    resp_s3_objects = s3_client.list_objects(Bucket = in_bucket)
    
    # only look at contents, list of dictionaries
    ls_s3_contents = resp_s3_objects['Contents']
    
    # Object names are stored in key called 'key'
    ls_s3_object_key = [x['Key'] for x in ls_s3_contents]
    
    # folders end in '/'
    ls_s3_folders = [x for x in ls_s3_object_key if x.endswith('/')]
    
    # files do not end in '/'
    ls_s3_files = [x for x in ls_s3_object_key if not x.endswith('/')]
    
    # optionally filter on files in folder in S3 bucket
    if in_folder:
        ls_s3_files = [x for x in ls_s3_files if x.startswith(in_folder)]
        
    # drop the folder name prefix, only provide the filename
    ls_s3_file_short = [x.split('/')[1] for x in ls_s3_files]
    
    return ls_s3_file_short
    

def get_ftp_to_s3(ip, username, password, in_bucket, in_folder):
    '''
    Log into FTP site, get details, print.
    Compare list of files in FTP to list of files in S3 bucket.
    Any new files in FTP, download from FTP, upload to S3.  
    Make sure to remove the files downloaded from FTP when done.  
    The files are zipped files.  Separate function to unzip.
    
    Dependencies:
        get_s3_file_names() - function to retrieve filenames from S3 bucket.
        
    Input:
        ip - str, address for FTP, e.g. 'L2.deltaneutral.net'
        username - str, username for FTP.  
        password - str, password for FTP.
        in_bucket - str, name of S3 bucket.
        in_folder - str, optional name of folder within S3 bucket.
        
    '''
    
    # FTP domain, username, and password
    print('Logging into FTP site: ')
    ftp_site = ftplib.FTP(ip)
    ftp_site.login(username, password)
    
    # show working directory, list directory contents
    print('Working directory: ')
    print(ftp_site.pwd())
    # print(ftp_site.dir())
    print()
    
    # change directory, show working directory, list directory contents
    ftp_site.cwd('Level2')
    print('Change working directory: ')
    print(ftp_site.pwd())
    # print(ftp_site.dir())
    print()
    
    # save contents in the FTP folder to list
    ls_ftp_contents = ftp_site.nlst()
    # print('Contents in FTP folder: ')
    # print(ls_ftp_contents)
    # print()
    
    # save zip files to list
    ls_ftp_zip_files = [x for x in ls_ftp_contents if x[-4:] == '.zip']
    # print('Zip files in FTP folder: ')
    # print(ls_ftp_zip_files)
    # print()
    
    # save monthly files to list
    # monthly files have underscore in 7th position, e.g. 'L2_2019_December.zip' or 'L2_2020_January.zip'
    ls_ftp_monthly_files = [x for x in ls_ftp_zip_files if x[7:8] == '_']
    # print('Monthly files: ')
    # print(ls_ftp_monthly_files)
    # print()
    
    # save daily files to list
    # daily files do not have an underscore in 7th position in string, after year, e.g. L2_20191220.zip
    ls_ftp_daily_files = [x for x in ls_ftp_zip_files if x[7:8] != '_']
    print('Getting list of daily files in FTP: ')
    print(f'Counted {len(ls_ftp_daily_files)} files.')
    # print(ls_ftp_daily_files)
    print()
    
    # check files in S3, to compare with files in FTP
    ls_s3_file_names = get_s3_file_names(in_bucket, in_folder)
    print('Getting list of files already in S3: ')
    print(f'Counted {len(ls_s3_file_names)} files.')
    # print(ls_s3_file_names)
    print()
    
    # print the list of files to download from FTP, not in S3 yet
    ls_get_ftp_to_s3 = list(set(ls_ftp_daily_files) - set(ls_s3_file_names))
    print('List of new files to download from FTP, not already in S3: ')
    print(ls_get_ftp_to_s3)
    print(f'Counted {len(ls_get_ftp_to_s3)} files.')
    print()
    
    if len(ls_get_ftp_to_s3)>0:
    
        # instantiate boto3-S3 client
        s3_client = boto3.client('s3')
        
        # Download each file from FTP, upload file to S3
        for each_file in ls_get_ftp_to_s3:
            
            # download file from FTP to local /tmp/ directory
            print(f"Downloading {each_file} from FTP ....")
            ftp_site.retrbinary(f"RETR {each_file}", open(each_file, 'wb').write)
            
            # upload file from local /tmp/ directory to S3 bucket
            print(f"Uploading {each_file} to S3 bucket {in_bucket}")
            # s3_client.upload_file('/tmp/' + each_file, s3_bucket, s3_folder + each_file)
            s3_client.upload_file(each_file, in_bucket, in_folder + each_file)
            
            
            # remove the file from local directory
            print(f'Removing file {each_file} from local directory')
            os.remove(each_file)
            print()
            
    print('# =================================================================')
    print('# Finished downloading files from FTP to S3, get_ftp_to_s3()')
    print('# =================================================================')
    print()
            
            
def unzip_s3_files(in_bucket, in_zipfolder, in_unzipfolder):
    '''
    Unzip the zipped S3 files, only the ones not yet unzipped.  Upload to S3.  
    
    Check the names of the files in the zip and unzip folders.
    The filenames we want in the unzip folder start with 'options_'.
    
    Dependencies:
        get_s3_file_names() - function to retrieve filenames from S3 bucket.
        
    Inputs:
        in_bucket - str, name of S3 bucket.
        in_zipfolder - str, name of folder in S3 bucket, stores zipped files.
        in_unzipfolder - str, name of folder in S3 bucket, stores unzipped files.
        
    Reference:
     * https://medium.com/@johnpaulhayes/how-extract-a-huge-zip-file-in-an-amazon-s3-bucket-by-using-aws-lambda-and-python-e32c6cf58f06
    '''
    
    # check files in S3
    ls_s3_zip_daily = get_s3_file_names(in_bucket, in_zipfolder)
    ls_s3_unzip_daily = get_s3_file_names(in_bucket, in_unzipfolder)
    
    # compare zip vs. unzip
    ls_options_unzip = [x for x in ls_s3_unzip_daily if x.startswith('options_')]
    ls_conform_unzip = [f'L2{x[7:-3]}zip' for x in ls_options_unzip]
    
    # generate difference, remaining files to unzip
    ls_diff_unzip = list(set(ls_s3_zip_daily) - set(ls_conform_unzip))
    print(f'Queue to unzip {len(ls_diff_unzip)} files.')
    print('ls_diff_unzip', ls_diff_unzip)
    print()
    
    # ==========================================================================
    # Unzip the remaining files, upload to S3
    # ==========================================================================
    
    # instantiate S3 resource object
    s3_resource = boto3.resource('s3')
    
    for each_zipfile in ls_diff_unzip:
        
        # store zip file in buffer
        zip_obj = s3_resource.Object(bucket_name=in_bucket, key=f'{in_zipfolder}{each_zipfile}')
        buffer = BytesIO(zip_obj.get()["Body"].read())

        # unzip all contents
        z = zipfile.ZipFile(buffer)
        for each_unzip_filename in z.namelist():
            # file_info = z.getinfo(each_unzipfile)
            s3_resource.meta.client.upload_fileobj(
                z.open(each_unzip_filename),
                Bucket=in_bucket,
                Key=f'{in_unzipfolder}{each_unzip_filename}'
                )
                
        print(f'Unzipped the file {each_zipfile}')
        
    print('# =================================================================')
    print('# Finished unzipping files in S3, unzip_s3_files()')
    print('# =================================================================')
    print()
    

if __name__=='__main__':
    
    # ===================================================================
    # Configuration for FTP DeltaNeutral and AWS S3 from credentials file
    # ===================================================================
    aws_secret_creds = "cred_deltaneutral"
    s3_bucket_name = "conifers"
    s3_folder_zip = 'zip_daily_files/'
    s3_folder_unzip = 'unzip_daily_files/'
    
    # get FTP credentials from AWS Secrets Manager as json string
    str_ftp_cred = get_secret(aws_secret_creds)
    
    # convert json string to python dictionary -> retrieve ip, username, password
    di_ftp_cred = json.loads(str_ftp_cred)
    ip = di_ftp_cred['ftp_address']
    username = di_ftp_cred['ftp_id']
    password = di_ftp_cred['ftp_pw']
    
    # download zip files from FTP, upload zip files to S3
    get_ftp_to_s3(ip, username, password, s3_bucket_name, s3_folder_zip)
    
    # unzip S3 files, upload contents to S3
    unzip_s3_files(s3_bucket_name, s3_folder_zip, s3_folder_unzip)