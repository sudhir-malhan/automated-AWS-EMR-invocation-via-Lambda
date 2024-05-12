# Automated-AWS-EMR-invocation-via-Lambda
This code provides capability to check in AWS Account if an EMR is already running/waiting or not. Based on the checks if there is No EMR running/waiting, then a New EMR with set configuration parameters is kicked off automatically using AWS Lambda and our SPARK Job is submitted for execution with logs written in our custom S3 bucket.
