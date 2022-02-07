#!/usr/bin/env python
# coding: utf-8

# In[3]:


import time 
from time import sleep
import json
from datetime import datetime
import boto3
import pandas as pd


# In[4]:


data_dir = "data"
get_ipython().system('mkdir $data_dir')

get_ipython().system('cd $data_dir && wget http://files.grouplens.org/datasets/movielens/ml-latest-small.zip')
get_ipython().system('cd $data_dir && unzip ml-latest-small.zip')
dataset_dir = data_dir + "/ml-latest-small/"
get_ipython().system('ls $dataset_dir')


# In[5]:


original_data = pd.read_csv(dataset_dir + '/ratings.csv')
print(original_data.info())
original_data.head()


# In[6]:


watched_df = original_data.copy()
watched_df = watched_df[watched_df['rating'] > 3]
watched_df = watched_df[['userId', 'movieId', 'timestamp']]
watched_df['EVENT_TYPE']='watch'

clicked_df = original_data.copy()
clicked_df = clicked_df[clicked_df['rating'] > 1]
clicked_df = clicked_df[['userId', 'movieId', 'timestamp']]
clicked_df['EVENT_TYPE']='click'

interactions_df = clicked_df.copy()
interactions_df = interactions_df.append(watched_df)
interactions_df.sort_values("timestamp", axis = 0, ascending = True, 
                 inplace = True, na_position ='last')


# In[7]:


interactions_df.rename(columns = {'userId':'USER_ID', 'movieId':'ITEM_ID', 
                              'timestamp':'TIMESTAMP'}, inplace = True) 
interactions_filename = "interactions.csv"
interactions_df.to_csv((data_dir+"/"+interactions_filename), index=False, float_format='%.0f')


# In[8]:


# Configure the SDK to Personalize:
personalize = boto3.client('personalize')
personalize_runtime = boto3.client('personalize-runtime')

create_dataset_group_response = personalize.create_dataset_group(
    name = "personalize-demo-movielens"
)

dataset_group_arn = create_dataset_group_response['datasetGroupArn']
print(json.dumps(create_dataset_group_response, indent=2))


# In[9]:


get_ipython().run_cell_magic('time', '', 'max_time = time.time() + 3*60*60 # 3 hours\nwhile time.time() < max_time:\n    describe_dataset_group_response = personalize.describe_dataset_group(\n        datasetGroupArn = dataset_group_arn\n    )\n    status = describe_dataset_group_response["datasetGroup"]["status"]\n    print("DatasetGroup: {}".format(status))\n    \n    if status == "ACTIVE" or status == "CREATE FAILED":\n        break\n        \n    time.sleep(60)')


# In[10]:


interactions_schema = {
    "type": "record",
    "name": "Interactions",
    "namespace": "com.amazonaws.personalize.schema",
    "fields": [
        {
            "name": "USER_ID",
            "type": "string"
        },
        {
            "name": "ITEM_ID",
            "type": "string"
        },
        {
            "name": "EVENT_TYPE",
            "type": "string"
        },
        {
            "name": "TIMESTAMP",
            "type": "long"
        }
    ],
    "version": "1.0"
}

create_schema_response = personalize.create_schema(
    name = "personalize-demo-movielens-interactions",
    schema = json.dumps(interactions_schema)
)

interaction_schema_arn = create_schema_response['schemaArn']
print(json.dumps(create_schema_response, indent=2))

dataset_type = "INTERACTIONS"
create_dataset_response = personalize.create_dataset(
    name = "personalize-demo-movielens-ints",
    datasetType = dataset_type,
    datasetGroupArn = dataset_group_arn,
    schemaArn = interaction_schema_arn
)

interactions_dataset_arn = create_dataset_response['datasetArn']
print(json.dumps(create_dataset_response, indent=2))


# In[11]:


session = boto3.session.Session()
region = session.region_name
s3 = boto3.client('s3')
account_id = boto3.client('sts').get_caller_identity().get('Account')
bucket_name = account_id + "-" + region + "-" + "personalizedemoml"
print(bucket_name)
if region == "us-east-1":
    s3.create_bucket(Bucket=bucket_name)
else:
    s3.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={'LocationConstraint': region}
        )


# In[12]:


interactions_file_path = data_dir + "/" + interactions_filename
boto3.Session().resource('s3').Bucket(bucket_name).Object(interactions_filename).upload_file(interactions_file_path)
interactions_s3DataPath = "s3://"+bucket_name+"/"+interactions_filename


# In[13]:


policy = {
    "Version": "2012-10-17",
    "Id": "PersonalizeS3BucketAccessPolicy",
    "Statement": [
        {
            "Sid": "PersonalizeS3BucketAccessPolicy",
            "Effect": "Allow",
            "Principal": {
                "Service": "personalize.amazonaws.com"
            },
            "Action": [
                "s3:*Object",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::{}".format(bucket_name),
                "arn:aws:s3:::{}/*".format(bucket_name)
            ]
        }
    ]
}

s3.put_bucket_policy(Bucket=bucket_name, Policy=json.dumps(policy))


# In[14]:


iam = boto3.client("iam")

role_name = "PersonalizeRolePOC"
assume_role_policy_document = {
    "Version": "2012-10-17",
    "Statement": [
        {
          "Effect": "Allow",
          "Principal": {
            "Service": "personalize.amazonaws.com"
          },
          "Action": "sts:AssumeRole"
        }
    ]
}

create_role_response = iam.create_role(
    RoleName = role_name,
    AssumeRolePolicyDocument = json.dumps(assume_role_policy_document)
)

# AmazonPersonalizeFullAccess provides access to any S3 bucket with a name that includes "personalize" or "Personalize" 
# if you would like to use a bucket with a different name, please consider creating and attaching a new policy
# that provides read access to your bucket or attaching the AmazonS3ReadOnlyAccess policy to the role
policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonPersonalizeFullAccess"
iam.attach_role_policy(
    RoleName = role_name,
    PolicyArn = policy_arn
)

# Now add S3 support
iam.attach_role_policy(
    PolicyArn='arn:aws:iam::aws:policy/AmazonS3FullAccess',
    RoleName=role_name
)
time.sleep(60) # wait for a minute to allow IAM role policy attachment to propagate

role_arn = create_role_response["Role"]["Arn"]
print(role_arn)


# In[15]:


create_dataset_import_job_response = personalize.create_dataset_import_job(
    jobName = "personalize-demo-import1",
    datasetArn = interactions_dataset_arn,
    dataSource = {
        "dataLocation": "s3://{}/{}".format(bucket_name, interactions_filename)
    },
    roleArn = role_arn
)

dataset_import_job_arn = create_dataset_import_job_response['datasetImportJobArn']
print(json.dumps(create_dataset_import_job_response, indent=2))


# In[16]:


get_ipython().run_cell_magic('time', '', 'max_time = time.time() + 6*60*60 # 6 hours\nwhile time.time() < max_time:\n    describe_dataset_import_job_response = personalize.describe_dataset_import_job(\n        datasetImportJobArn = dataset_import_job_arn\n    )\n    status = describe_dataset_import_job_response["datasetImportJob"][\'status\']\n    print("DatasetImportJob: {}".format(status))\n    \n    if status == "ACTIVE" or status == "CREATE FAILED":\n        break\n        \n    time.sleep(60)')


# In[17]:


# aws-user-personalization selected for demo purposes
recipe_arn = "arn:aws:personalize:::recipe/aws-user-personalization"


# In[18]:


create_solution_response = personalize.create_solution(
    name = "personalize-demo-soln-user-personalization",
    datasetGroupArn = dataset_group_arn,
    recipeArn = recipe_arn
)

solution_arn = create_solution_response['solutionArn']
print(json.dumps(create_solution_response, indent=2))


# In[19]:


create_solution_version_response = personalize.create_solution_version(
    solutionArn = solution_arn
)

solution_version_arn = create_solution_version_response['solutionVersionArn']
print(json.dumps(create_solution_version_response, indent=2))


# In[20]:


get_ipython().run_cell_magic('time', '', 'max_time = time.time() + 3*60*60 # 3 hours\nwhile time.time() < max_time:\n    describe_solution_version_response = personalize.describe_solution_version(\n        solutionVersionArn = solution_version_arn\n    )\n    status = describe_solution_version_response["solutionVersion"]["status"]\n    print("SolutionVersion: {}".format(status))\n    \n    if status == "ACTIVE" or status == "CREATE FAILED":\n        break\n        \n    time.sleep(60)')


# In[21]:


get_solution_metrics_response = personalize.get_solution_metrics(
    solutionVersionArn = solution_version_arn
)

print(json.dumps(get_solution_metrics_response, indent=2))


# In[22]:


create_campaign_response = personalize.create_campaign(
    name = "personalize-demo-camp",
    solutionVersionArn = solution_version_arn,
    minProvisionedTPS = 1,
    campaignConfig = {
        "itemExplorationConfig": {
            "explorationWeight": "0.3",
	"explorationItemAgeCutOff": "30"
        }
    }
)

campaign_arn = create_campaign_response['campaignArn']
print(json.dumps(create_campaign_response, indent=2))

max_time = time.time() + 3*60*60 # 3 hours
while time.time() < max_time:
    describe_campaign_response = personalize.describe_campaign(
        campaignArn = campaign_arn
    )
    status = describe_campaign_response["campaign"]["status"]
    print("Campaign: {}".format(status))
    
    if status == "ACTIVE" or status == "CREATE FAILED":
        break
        
    time.sleep(60)


# In[23]:


# Build a map to convert a movie id to the movie title
movies = pd.read_csv(dataset_dir + '/movies.csv', usecols=[0,1])
movies['movieId'] = movies['movieId'].astype(str)
movie_map = dict(movies.values)

# Getting a random user:
user_id, item_id = interactions_df[['USER_ID', 'ITEM_ID']].sample().values[0]

get_recommendations_response = personalize_runtime.get_recommendations(
    campaignArn = campaign_arn,
    userId = str(user_id),
)
# Update DF rendering
pd.set_option('display.max_rows', 30)

print("Recommendations for user: ", user_id)

item_list = get_recommendations_response['itemList']

recommendation_list = []

for item in item_list:
    title = movie_map[item['itemId']]
    recommendation_list.append(title)
    
recommendations_df = pd.DataFrame(recommendation_list, columns = ['OriginalRecs'])
recommendations_df.head()


# In[ ]:




