### Prerequisites (ie one-time setup)

1. Create an ECR repo and grant the `METAFLOW_ECS_S3_ACCESS_IAM_ROLE` permissions to access ECR as well as EMR serverless.
Refer to https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/application-custom-image.html for details.

2. Deploy custom Docker image to ECR:

```
cd docker
ECR_REPO_NAME=<YOUR-ECR-REPO-NAME> ./build.sh
```

3. Create an EMR serverless application using the custom image created above.

4. Edit `spark_config.json` with your EMR serverless application ID and execution role.


### Running
1. First deploy the flow to SFN:
```
python sparkflow.py --environment=conda step-functions create
```

2. Trigger the SFN:

```
REGION=$(aws configure get region)

# Get the current AWS account ID
ACCOUNT_ID=$(aws sts get-caller-identity --query 'Account' --output text)

aws stepfunctions start-execution --state-machine-arn "arn:aws:states:$REGION:$ACCOUNT_ID":stateMachine:PySparkTestFlow --input "{\"Parameters\": \"{}\"}"
```
