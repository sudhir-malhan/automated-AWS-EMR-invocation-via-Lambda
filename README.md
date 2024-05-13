# Automated-AWS-EMR-invocation-via-Lambda

**Problem Statement:-**
Business wants to cut down IT resource costs of persistent AWS EMRs (an expensive AWS services) that are up and running 24x7 even when there are No jobs running or waiting in the queue.


**Solution:-**
Check and utilize an existing/waiting AWS EMR Or spin-up a New AWS EMR on Adhoc basis in order to execute and finish the SPARK jobs and spins down the EMR cluster once SPARK job(s) complete and the jobs queue is empty.

Description:-
This code provides capability to check in AWS Account if an EMR is already running/waiting or not. Based on the checks if there is No EMR running/waiting, then a New EMR with set configuration parameters is kicked off automatically using AWS Lambda and our SPARK Job is submitted for execution with logs written in our custom S3 bucket.
