# %%
import boto3
from botocore.exceptions import NoCredentialsError
import os
from dotenv import load_dotenv
from pathlib import Path
import sys

# %%
load_dotenv()

# %%
FILENAME = f"{sys.argv[1]}_author"
print(FILENAME)
# %%

def upload_to_aws(local_file, bucket, s3_file):
    s3 = boto3.client('s3', aws_access_key_id= os.environ.get('AWS_ACCESS_KEY_ID'),
                      aws_secret_access_key= os.environ.get('AWS_SECRET_ACCESS_KEY'))

    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False


uploaded = upload_to_aws(f'/media/user/volume2/students/s121md210_02/singaporesub/Data/{FILENAME}.csv', 'initial-data-load-bucket', f'{FILENAME}.csv')




# %%


# %%



