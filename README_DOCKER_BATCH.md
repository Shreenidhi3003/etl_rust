# Docker and AWS Batch

This project builds a container image for the `xmlpoc` Rust binary and runs it as a one-shot AWS Batch job.

## Build Locally

```powershell
docker build -t xmlpoc:latest .
```

## Run Locally

Use local AWS credentials from your environment or mounted AWS profile:

```powershell
docker run --rm `
  -e AWS_REGION=ap-south-1 `
  -e AWS_ACCESS_KEY_ID=$env:AWS_ACCESS_KEY_ID `
  -e AWS_SECRET_ACCESS_KEY=$env:AWS_SECRET_ACCESS_KEY `
  -e AWS_SESSION_TOKEN=$env:AWS_SESSION_TOKEN `
  xmlpoc:latest
```

For AWS Batch, do not pass access keys. Attach an IAM job role with S3 permissions instead.

Required S3 permissions for the job role:

- `s3:ListBucket` on the input bucket
- `s3:GetObject` on input XML objects
- `s3:PutObject` on output CSV objects

The current buckets and prefixes are compiled from `src/config.rs`.

## Push To Amazon ECR

Set these values for your AWS account and region:

```powershell
$AWS_REGION = "ap-south-1"
$AWS_ACCOUNT_ID = "<account-id>"
$REPOSITORY = "xmlpoc"
$IMAGE_TAG = "latest"
$IMAGE_URI = "$AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$REPOSITORY`:$IMAGE_TAG"
```

Create the repository once:

```powershell
aws ecr create-repository --repository-name $REPOSITORY --region $AWS_REGION
```

Login, tag, and push:

```powershell
aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin "$AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com"
docker tag xmlpoc:latest $IMAGE_URI
docker push $IMAGE_URI
```

## Register AWS Batch Job Definition

Replace the placeholders before running:

```powershell
aws batch register-job-definition `
  --job-definition-name xmlpoc-job `
  --type container `
  --platform-capabilities FARGATE `
  --container-properties "{
    `"image`": `"$IMAGE_URI`",
    `"jobRoleArn`": `"arn:aws:iam::<account-id>:role/<batch-job-role>`",
    `"executionRoleArn`": `"arn:aws:iam::<account-id>:role/<ecs-task-execution-role>`",
    `"resourceRequirements`": [
      {`"type`": `"VCPU`", `"value`": `"1`"},
      {`"type`": `"MEMORY`", `"value`": `"2048`"}
    ],
    `"environment`": [
      {`"name`": `"AWS_REGION`", `"value`": `"$AWS_REGION`"}
    ],
    `"logConfiguration`": {
      `"logDriver`": `"awslogs`"
    }
  }" `
  --region $AWS_REGION
```

## Submit The Batch Job

Replace the queue name with your Batch job queue:

```powershell
aws batch submit-job `
  --job-name xmlpoc-run `
  --job-queue <batch-job-queue-name> `
  --job-definition xmlpoc-job `
  --region $AWS_REGION
```

Logs will appear in CloudWatch Logs for the Batch job.
