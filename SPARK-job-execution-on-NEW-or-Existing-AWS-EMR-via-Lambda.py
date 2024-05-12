import sys
import time
import boto3
import json
 

def lambda_handler(event, context):

    conn = boto3.client('emr')

    # chooses the first cluster which is Running or Waiting
    # possibly can also choose by name or already have the cluster id

    clusters = conn.list_clusters()

    # choose the correct cluster
    clusters = [c["Id"] for c in clusters["Clusters"]
                if c["Status"]["State"] in ["RUNNING", "WAITING"]]

    # take the first relevant cluster

    if clusters:
      print(clusters)
      cluster_id = clusters[0]


    if not clusters:
        cluster_id = conn.run_job_flow(
        Name='CIA ON-DEMAND Cluster',
        ServiceRole='EMR_DefaultRole',
        JobFlowRole='EMR_EC2_DefaultRole',
        VisibleToAllUsers=True,
        LogUri='s3://<test-bucket-name>/<main folder name>/elasticmapreduce/logs/',

        ReleaseLabel='emr-5.8.0',

        Instances={

            'InstanceGroups': [

                {
                    'Name': 'Master nodes',

                    'Market': 'ON_DEMAND',

                    'InstanceRole': 'MASTER',

                    'InstanceType': 'm3.xlarge',

                    'InstanceCount': 1
                },

                {
                    'Name': 'Slave nodes',

                    'Market': 'ON_DEMAND',

                    'InstanceRole': 'CORE',

                    'InstanceType': 'm3.xlarge',

                    'InstanceCount': 2
                }
            ],

            'Ec2KeyName': 'key-name',

            'KeepJobFlowAliveWhenNoSteps': False,

            'TerminationProtected': False
        },

        Applications=[{
            'Name': 'Spark'
        }],

        Configurations=[{

            "Classification":"spark-env",

            "Properties":{},

            "Configurations":[{

                "Classification":"export",

                "Properties":{
                    "PYSPARK_PYTHON":"python35",
                    "PYSPARK_DRIVER_PYTHON":"python35"
                }
            }]
        }],

        BootstrapActions=[{

            'Name': 'Install',

            'ScriptBootstrapAction': {

                'Path': 's3://path/to/bootstrap.script'
            }
        }]

        )

    return "Started cluster {}".format(cluster_id)


    s3_client = boto3.client('s3')

    bucket = event['Records'][0]['s3']['bucket']['name']

    json_file_name= event['Records'][0]['s3']['object']['key']

    s3 = boto3.resource('s3')

    content_object = s3.Object(bucket, json_file_name)

    file_content = content_object.get()['Body'].read().decode('utf-8')

    json_content = json.loads(file_content)

    cia_class= json_content['class']

    print(cia_class)

    cluster_id = '<cluster-id>'

    CODE_DIR = 's3://<your-s3-bucket-name>/<main-folder-name>/'

    step_args = ['/usr/bin/spark-submit', '--deploy-mode', 'cluster', CODE_DIR + 'test.py']
   
    step = {'Name': 'KICKOFF CIA AWS LAMBDA' + time.strftime('%Y%m%d-%H:%M'),

    'ActionOnFailure': 'CONTINUE',

    'HadoopJarStep': {

        'Jar': 's3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar',

        'Args': step_args
    }

    }

    action = conn.add_job_flow_steps(JobFlowId=cluster_id, Steps=[step])
    print(step)

    return 'Added step: %s'%(action)
