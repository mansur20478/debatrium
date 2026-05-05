"""
to setup the AWS infrastructure for the distributed debate system. This includes:
- VPC with 2 subnets across AZs
- Security groups for Lambda and ElastiCache
- VPC endpoints for SQS, SSM, Lambda (private API access)
- SQS FIFO queues with DLQs
- S3 bucket for results
- ElastiCache Redis cluster for shared state
- Storing config and secrets in SSM Parameter Store
- Attaching the aggregator Lambda to the VPC and setting env vars   
"""

import boto3
import json
import time
import sys
from typing import Dict, Optional
from botocore.exceptions import ClientError

# CONFIGURATION

REGION     = "us-east-1"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]

# VPC
VPC_CIDR             = "10.0.0.0/16"
SUBNET_CIDRS         = ["10.0.1.0/24", "10.0.2.0/24"]   
AVAILABILITY_ZONES   = ["us-east-1a", "us-east-1b"]
VPC_NAME             = f"debate-vpc-{ACCOUNT_ID[-6:]}"

# Security groups
LAMBDA_SG_NAME       = f"debate-lambda-sg-{ACCOUNT_ID[-6:]}"
ELASTICACHE_SG_NAME  = f"debate-redis-sg-{ACCOUNT_ID[-6:]}"

# Queues
QUEUES = {
    "research_tasks":   f"research-tasks-{ACCOUNT_ID[-6:]}.fifo",
    "research_results": f"research-results-{ACCOUNT_ID[-6:]}.fifo",
    "critic_tasks":     f"critic-tasks-{ACCOUNT_ID[-6:]}.fifo",
    "judge_tasks":      f"judge-tasks-{ACCOUNT_ID[-6:]}.fifo",
    "final_results":    f"final-results-{ACCOUNT_ID[-6:]}.fifo",
}
DLQS = {
    "research_tasks_dlq":   f"research-tasks-dlq-{ACCOUNT_ID[-6:]}.fifo",
    "research_results_dlq": f"research-results-dlq-{ACCOUNT_ID[-6:]}.fifo",
    "critic_tasks_dlq":     f"critic-tasks-dlq-{ACCOUNT_ID[-6:]}.fifo",
    "judge_tasks_dlq":      f"judge-tasks-dlq-{ACCOUNT_ID[-6:]}.fifo",
    "final_results_dlq":    f"final-results-dlq-{ACCOUNT_ID[-6:]}.fifo",
}

# S3
BUCKET = f"debate-results-{ACCOUNT_ID[-12:]}"

# ElastiCache
ELASTICACHE_CLUSTER_ID     = f"debate-redis-{ACCOUNT_ID[-6:]}"
ELASTICACHE_NODE_TYPE      = "cache.t3.micro"
ELASTICACHE_ENGINE         = "redis"
ELASTICACHE_ENGINE_VERSION = "7.0"
ELASTICACHE_NUM_REPLICAS   = 1
ELASTICACHE_SUBNET_GROUP   = f"debate-redis-subnet-{ACCOUNT_ID[-6:]}"

# Lambda
LAMBDA_FUNCTION  = "aggregator_lambda"

# ESM
BATCH_SIZE      = 10
MAX_CONCURRENCY = 5

# SSM paths
SSM_PATHS = {
    "tasks_queue_url":          "/research-agent/tasks-queue-url",
    "results_queue_url":        "/research-agent/results-queue-url",
    "openai_api_key":           "/research-agent/openai-api-key",
    "config_json":              "/research-agent/config",
    "critic_tasks_queue_url":   "/critic-agent/tasks-queue-url",
    "critic_results_queue_url": "/critic-agent/results-queue-url",
    "critic_openai_api_key":    "/critic-agent/openai-api-key",
    "critic_config_json":       "/critic-agent/config",
    "redis_host":               "/debate/redis-host",
    "redis_port":               "/debate/redis-port",
    "redis_auth_token":         "/debate/redis-auth-token",
}


# CLIENTS

ec2          = boto3.client("ec2",          region_name=REGION)
sqs          = boto3.client("sqs",          region_name=REGION)
s3           = boto3.client("s3",           region_name=REGION)
ssm          = boto3.client("ssm",          region_name=REGION)
elasticache  = boto3.client("elasticache",  region_name=REGION)
lambda_      = boto3.client("lambda",       region_name=REGION)


# PRINT HELPERS

def log(msg):    print(f"  → {msg}")
def ok(msg):     print(f"  ✓ {msg}")
def skip(msg):   print(f"  ≡ {msg} (already exists)")
def warn(msg):   print(f"  ⚠ {msg}")
def fail(msg):   print(f"  ✗ {msg}")
def section(msg): print(f"\n{'='*60}\n  {msg}\n{'='*60}")



# STEP 1 — CREDENTIALS CHECK

def check_credentials():
    section("Step 1 — AWS Credentials")
    try:
        identity = boto3.client("sts").get_caller_identity()
        ok(f"Account : {identity['Account']}")
        ok(f"Role    : {identity['Arn']}")
    except Exception as e:
        fail(f"Credentials error: {e}")
        sys.exit(1)



# STEP 2 — VPC

def setup_vpc() -> Dict:
 
    section("Step 2 — VPC & Networking")

    # Check if VPC already exists by name tag
    vpcs = ec2.describe_vpcs(
        Filters=[{"Name": "tag:Name", "Values": [VPC_NAME]}]
    )["Vpcs"]

    if vpcs:
        vpc_id = vpcs[0]["VpcId"]
        skip(f"VPC: {vpc_id}")
    else:
        log(f"Creating VPC: {VPC_CIDR}")
        vpc = ec2.create_vpc(CidrBlock=VPC_CIDR)["Vpc"]
        vpc_id = vpc["VpcId"]
        ec2.create_tags(Resources=[vpc_id], Tags=[
            {"Key": "Name",    "Value": VPC_NAME},
            {"Key": "Project", "Value": "Debatrium"},
        ])
        # Enable DNS so Lambda can resolve VPC endpoint hostnames
        ec2.modify_vpc_attribute(VpcId=vpc_id, EnableDnsSupport={"Value": True})
        ec2.modify_vpc_attribute(VpcId=vpc_id, EnableDnsHostnames={"Value": True})
        ok(f"VPC created: {vpc_id}")

    # Subnets
    subnet_ids = []
    for i, (cidr, az) in enumerate(zip(SUBNET_CIDRS, AVAILABILITY_ZONES)):
        existing = ec2.describe_subnets(
            Filters=[
                {"Name": "vpc-id",            "Values": [vpc_id]},
                {"Name": "cidrBlock",         "Values": [cidr]},
            ]
        )["Subnets"]
        if existing:
            subnet_ids.append(existing[0]["SubnetId"])
            skip(f"Subnet {i+1}: {existing[0]['SubnetId']} ({cidr} / {az})")
        else:
            log(f"Creating subnet {i+1}: {cidr} in {az}")
            subnet = ec2.create_subnet(
                VpcId=vpc_id, CidrBlock=cidr, AvailabilityZone=az
            )["Subnet"]
            subnet_id = subnet["SubnetId"]
            ec2.create_tags(Resources=[subnet_id], Tags=[
                {"Key": "Name", "Value": f"debate-subnet-{i+1}"},
            ])
            subnet_ids.append(subnet_id)
            ok(f"Subnet {i+1}: {subnet_id}")

    # Internet Gateway
    igws = ec2.describe_internet_gateways(
        Filters=[{"Name": "attachment.vpc-id", "Values": [vpc_id]}]
    )["InternetGateways"]
    if igws:
        igw_id = igws[0]["InternetGatewayId"]
        skip(f"IGW: {igw_id}")
    else:
        log("Creating Internet Gateway")
        igw = ec2.create_internet_gateway()["InternetGateway"]
        igw_id = igw["InternetGatewayId"]
        ec2.attach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)
        ec2.create_tags(Resources=[igw_id], Tags=[
            {"Key": "Name", "Value": f"debate-igw-{ACCOUNT_ID[-6:]}"},
        ])
        ok(f"IGW created and attached: {igw_id}")

    # Route table — attach to all subnets
    rts = ec2.describe_route_tables(
        Filters=[
            {"Name": "vpc-id",         "Values": [vpc_id]},
            {"Name": "tag:Name",       "Values": ["debate-rt"]},
        ]
    )["RouteTables"]
    if rts:
        rt_id = rts[0]["RouteTableId"]
        skip(f"Route table: {rt_id}")
    else:
        log("Creating route table")
        rt = ec2.create_route_table(VpcId=vpc_id)["RouteTable"]
        rt_id = rt["RouteTableId"]
        ec2.create_tags(Resources=[rt_id], Tags=[{"Key": "Name", "Value": "debate-rt"}])
        ec2.create_route(RouteTableId=rt_id, DestinationCidrBlock="0.0.0.0/0",
                         GatewayId=igw_id)
        ok(f"Route table created: {rt_id}")

    # Associate route table with all subnets
    for subnet_id in subnet_ids:
        assocs = ec2.describe_route_tables(
            Filters=[{"Name": "association.subnet-id", "Values": [subnet_id]}]
        )["RouteTables"]
        if not assocs:
            ec2.associate_route_table(RouteTableId=rt_id, SubnetId=subnet_id)
            ok(f"Route table associated with subnet {subnet_id}")
        else:
            skip(f"Route table already associated with {subnet_id}")

    ok(f"VPC ready — {vpc_id} | subnets: {subnet_ids}")
    return {"vpc_id": vpc_id, "subnet_ids": subnet_ids, "route_table_id": rt_id}



# STEP 3 — SECURITY GROUPS

def setup_security_groups(vpc_id: str) -> Dict:
    """
    Create two security groups:
    1. Lambda/agent SG — all outbound, inbound only from itself
    2. ElastiCache SG  — inbound 6379 from Lambda SG only

    Returns {lambda_sg_id, elasticache_sg_id}
    """
    section("Step 3 — Security Groups")

    def get_sg(name):
        sgs = ec2.describe_security_groups(
            Filters=[
                {"Name": "group-name", "Values": [name]},
                {"Name": "vpc-id",     "Values": [vpc_id]},
            ]
        )["SecurityGroups"]
        return sgs[0]["GroupId"] if sgs else None

    # ── Lambda / agent security group ────────────────────────
    lambda_sg_id = get_sg(LAMBDA_SG_NAME)
    if lambda_sg_id:
        skip(f"Lambda SG: {lambda_sg_id}")
    else:
        log(f"Creating Lambda security group: {LAMBDA_SG_NAME}")
        sg = ec2.create_security_group(
            GroupName=LAMBDA_SG_NAME,
            Description="Debate system - Lambda and EC2 agents",
            VpcId=vpc_id,
        )
        lambda_sg_id = sg["GroupId"]
        ec2.create_tags(Resources=[lambda_sg_id], Tags=[
            {"Key": "Name", "Value": LAMBDA_SG_NAME}
        ])
        # Outbound: all traffic
        # (Default outbound rule already allows all — no action needed)
        # Inbound: self-referencing (agents talking to each other)
        ec2.authorize_security_group_ingress(
            GroupId=lambda_sg_id,
            IpPermissions=[
                {
                    "IpProtocol": "-1",
                    "UserIdGroupPairs": [{"GroupId": lambda_sg_id}],
                },
                # HTTPS inbound for VPC endpoint traffic
                {
                    "IpProtocol": "tcp",
                    "FromPort":   443,
                    "ToPort":     443,
                    "IpRanges":   [{"CidrIp": "0.0.0.0/0"}],
                },
            ]
        )
        ok(f"Lambda SG created: {lambda_sg_id}")

    # ── ElastiCache security group ────────────────────────────
    redis_sg_id = get_sg(ELASTICACHE_SG_NAME)
    if redis_sg_id:
        skip(f"ElastiCache SG: {redis_sg_id}")
    else:
        log(f"Creating ElastiCache security group: {ELASTICACHE_SG_NAME}")
        sg = ec2.create_security_group(
            GroupName=ELASTICACHE_SG_NAME,
            Description="Debate system - ElastiCache Redis",
            VpcId=vpc_id,
        )
        redis_sg_id = sg["GroupId"]
        ec2.create_tags(Resources=[redis_sg_id], Tags=[
            {"Key": "Name", "Value": ELASTICACHE_SG_NAME}
        ])
        # Inbound: Redis port from Lambda SG only
        ec2.authorize_security_group_ingress(
            GroupId=redis_sg_id,
            IpPermissions=[
                {
                    "IpProtocol": "tcp",
                    "FromPort":   6379,
                    "ToPort":     6379,
                    "UserIdGroupPairs": [{"GroupId": lambda_sg_id}],
                }
            ]
        )
        ok(f"ElastiCache SG created: {redis_sg_id}")

    return {"lambda_sg_id": lambda_sg_id, "elasticache_sg_id": redis_sg_id}



# STEP 4 — VPC ENDPOINTS

def setup_vpc_endpoints(vpc_id: str, subnet_ids: list, lambda_sg_id: str, route_table_id: str):
    """
    Create Interface VPC endpoints for SQS and SSM so Lambda/agents
    can reach AWS APIs without going through the public internet.
    Private DNS is enabled so boto3 just works — no URL changes needed.

    Also creates a Gateway endpoint for S3 (free, no SG needed).
    """
    section("Step 4 — VPC Endpoints")

    def endpoint_exists(service_name):
        eps = ec2.describe_vpc_endpoints(
            Filters=[
                {"Name": "service-name", "Values": [service_name]},
                {"Name": "vpc-id",       "Values": [vpc_id]},
                {"Name": "vpc-endpoint-state", "Values": ["available", "pending"]},
            ]
        )["VpcEndpoints"]
        return eps[0]["VpcEndpointId"] if eps else None

    interface_services = {
        "SQS": f"com.amazonaws.{REGION}.sqs",
        "SSM": f"com.amazonaws.{REGION}.ssm",
        "SSM Messages": f"com.amazonaws.{REGION}.ssmmessages",
        "Lambda": f"com.amazonaws.{REGION}.lambda",
    }

    endpoint_ids = []
    for name, service in interface_services.items():
        ep_id = endpoint_exists(service)
        if ep_id:
            skip(f"{name} endpoint: {ep_id}")
            # Make sure private DNS is enabled
            try:
                ec2.modify_vpc_endpoint(
                    VpcEndpointId=ep_id,
                    PrivateDnsEnabled=True,
                )
            except Exception:
                pass
            endpoint_ids.append(ep_id)
        else:
            log(f"Creating {name} Interface endpoint")
            ep = ec2.create_vpc_endpoint(
                VpcEndpointType="Interface",
                VpcId=vpc_id,
                ServiceName=service,
                SubnetIds=subnet_ids,
                SecurityGroupIds=[lambda_sg_id],
                PrivateDnsEnabled=True,
            )
            ep_id = ep["VpcEndpoint"]["VpcEndpointId"]
            endpoint_ids.append(ep_id)
            ok(f"{name} endpoint created: {ep_id}")

    # Gateway endpoint for S3 (free, attaches to route table)
    s3_service = f"com.amazonaws.{REGION}.s3"
    s3_ep_id = endpoint_exists(s3_service)
    if s3_ep_id:
        skip(f"S3 Gateway endpoint: {s3_ep_id}")
    else:
        log("Creating S3 Gateway endpoint")
        ep = ec2.create_vpc_endpoint(
            VpcEndpointType="Gateway",
            VpcId=vpc_id,
            ServiceName=s3_service,
            RouteTableIds=[route_table_id],
        )
        ok(f"S3 Gateway endpoint created: {ep['VpcEndpoint']['VpcEndpointId']}")

    ok("All VPC endpoints ready")
    return endpoint_ids



# STEP 5 — SQS QUEUES

def get_queue_url(queue_name: str) -> Optional[str]:
    try:
        return sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
    except sqs.exceptions.QueueDoesNotExist:
        return None

def create_fifo_queue(name: str, dlq_arn: Optional[str] = None) -> str:
    attrs = {
        "FifoQueue":                 "true",
        "ContentBasedDeduplication": "true",
        "VisibilityTimeout":         "300",   # 5 min — matches Lambda timeout
        "MessageRetentionPeriod":    "345600",
    }
    if dlq_arn:
        attrs["RedrivePolicy"] = json.dumps({
            "deadLetterTargetArn": dlq_arn,
            "maxReceiveCount":     "3"
        })
    return sqs.create_queue(QueueName=name, Attributes=attrs)["QueueUrl"]

def setup_queues() -> Dict[str, str]:
    section("Step 5 — SQS FIFO Queues")
    queue_urls = {}

    dlq_arns = {}
    for dlq_key, dlq_name in DLQS.items():
        existing_url = get_queue_url(dlq_name)
        if existing_url:
            skip(f"DLQ: {dlq_name}")
            arn = sqs.get_queue_attributes(
                QueueUrl=existing_url, AttributeNames=["QueueArn"]
            )["Attributes"]["QueueArn"]
        else:
            log(f"Creating DLQ: {dlq_name}")
            url = create_fifo_queue(dlq_name)
            arn = sqs.get_queue_attributes(
                QueueUrl=url, AttributeNames=["QueueArn"]
            )["Attributes"]["QueueArn"]
            ok(f"DLQ created: {dlq_name}")
        dlq_arns[dlq_name] = arn

    main_to_dlq = {
        "research_tasks":   "research_tasks_dlq",
        "research_results": "research_results_dlq",
        "critic_tasks":     "critic_tasks_dlq",
        "judge_tasks":      "judge_tasks_dlq",
        "final_results":    "final_results_dlq",
    }

    for main_name, queue_name in QUEUES.items():
        dlq_arn = dlq_arns[DLQS[main_to_dlq[main_name]]]
        existing_url = get_queue_url(queue_name)
        if existing_url:
            queue_urls[main_name] = existing_url
            skip(f"Queue: {queue_name}")
        else:
            log(f"Creating queue: {queue_name}")
            url = create_fifo_queue(queue_name, dlq_arn)
            queue_urls[main_name] = url
            ok(f"Queue created: {queue_name}")
        time.sleep(0.5)

    return queue_urls



# STEP 6 — S3

def setup_s3():
    section("Step 6 — S3 Bucket")
    try:
        s3.head_bucket(Bucket=BUCKET)
        skip(f"Bucket: {BUCKET}")
        return
    except Exception:
        pass

    log(f"Creating bucket: {BUCKET}")
    if REGION == "us-east-1":
        s3.create_bucket(Bucket=BUCKET)
    else:
        s3.create_bucket(
            Bucket=BUCKET,
            CreateBucketConfiguration={"LocationConstraint": REGION}
        )
    s3.put_public_access_block(
        Bucket=BUCKET,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True, "IgnorePublicAcls": True,
            "BlockPublicPolicy": True, "RestrictPublicBuckets": True,
        }
    )
    ok(f"Bucket created: {BUCKET}")



# STEP 7 — ELASTICACHE

def setup_elasticache(subnet_ids: list, elasticache_sg_id: str) -> Dict:
    section("Step 7 — ElastiCache Redis")

    # Subnet group
    try:
        elasticache.describe_cache_subnet_groups(
            CacheSubnetGroupName=ELASTICACHE_SUBNET_GROUP
        )
        skip(f"Subnet group: {ELASTICACHE_SUBNET_GROUP}")
    except elasticache.exceptions.CacheSubnetGroupNotFoundFault:
        log(f"Creating ElastiCache subnet group: {ELASTICACHE_SUBNET_GROUP}")
        elasticache.create_cache_subnet_group(
            CacheSubnetGroupName=ELASTICACHE_SUBNET_GROUP,
            CacheSubnetGroupDescription="Debate system Redis subnet group",
            SubnetIds=subnet_ids,
        )
        ok(f"Subnet group created: {ELASTICACHE_SUBNET_GROUP}")

    # Replication group
    try:
        resp   = elasticache.describe_replication_groups(
            ReplicationGroupId=ELASTICACHE_CLUSTER_ID
        )
        group  = resp["ReplicationGroups"][0]
        status = group["Status"]

        if status == "available":
            endpoint = group["NodeGroups"][0]["PrimaryEndpoint"]
            host     = endpoint["Address"]
            port     = str(endpoint["Port"])
            skip(f"Redis cluster: {ELASTICACHE_CLUSTER_ID} ({host}:{port})")
            return {"host": host, "port": port}

        warn(f"Cluster exists but status='{status}' — waiting...")

    except elasticache.exceptions.ReplicationGroupNotFoundFault:
        log(f"Creating ElastiCache Redis: {ELASTICACHE_CLUSTER_ID}")
        elasticache.create_replication_group(
            ReplicationGroupId=           ELASTICACHE_CLUSTER_ID,
            ReplicationGroupDescription=  "Debate system shared Redis state",
            NumCacheClusters=             1 + ELASTICACHE_NUM_REPLICAS,
            CacheNodeType=                ELASTICACHE_NODE_TYPE,
            Engine=                       ELASTICACHE_ENGINE,
            EngineVersion=                ELASTICACHE_ENGINE_VERSION,
            CacheSubnetGroupName=         ELASTICACHE_SUBNET_GROUP,
            SecurityGroupIds=             [elasticache_sg_id],
            AutomaticFailoverEnabled=     ELASTICACHE_NUM_REPLICAS > 0,
            MultiAZEnabled=               ELASTICACHE_NUM_REPLICAS > 0,
            AtRestEncryptionEnabled=      True,
            TransitEncryptionEnabled=     True,
            Tags=[
                {"Key": "Project", "Value": "Debatrium"},
                {"Key": "Role",    "Value": "shared-state"},
            ]
        )
        ok(f"Redis cluster creation initiated — this takes ~10 min")

    print(f"  Polling until available...")
    for attempt in range(80):
        time.sleep(15)
        try:
            resp   = elasticache.describe_replication_groups(
                ReplicationGroupId=ELASTICACHE_CLUSTER_ID
            )
            group  = resp["ReplicationGroups"][0]
            status = group["Status"]
            print(f"  [{attempt+1}/80] Status: {status}")
            if status == "available":
                endpoint = group["NodeGroups"][0]["PrimaryEndpoint"]
                host     = endpoint["Address"]
                port     = str(endpoint["Port"])
                ok(f"Redis ready — {host}:{port}")
                return {"host": host, "port": port}
        except Exception as e:
            warn(f"Poll error: {e}")

    fail("Timed out waiting for ElastiCache.")
    sys.exit(1)



# STEP 8 — SSM PARAMETER STORE

def store_in_ssm(path: str, value: str, is_secure: bool = False):
    try:
        ssm.put_parameter(
            Name=path, Value=value,
            Type="SecureString" if is_secure else "String",
            Overwrite=True,
        )
        ok(f"SSM: {path}")
    except Exception as e:
        fail(f"SSM failed [{path}]: {e}")

def setup_ssm(queue_urls: Dict, redis_info: Dict):
    section("Step 8 — SSM Parameter Store")

    store_in_ssm(SSM_PATHS["tasks_queue_url"],          queue_urls["research_tasks"])
    store_in_ssm(SSM_PATHS["results_queue_url"],         queue_urls["research_results"])
    store_in_ssm(SSM_PATHS["critic_tasks_queue_url"],    queue_urls["critic_tasks"])
    store_in_ssm(SSM_PATHS["critic_results_queue_url"],  queue_urls["judge_tasks"])
    store_in_ssm(SSM_PATHS["redis_host"],                redis_info["host"])
    store_in_ssm(SSM_PATHS["redis_port"],                redis_info["port"])

    research_config = {
        "region": REGION, "account_id": ACCOUNT_ID,
        "queues": queue_urls, "bucket": BUCKET, "ssm_paths": SSM_PATHS,
    }
    store_in_ssm(SSM_PATHS["config_json"],       json.dumps(research_config))
    store_in_ssm(SSM_PATHS["critic_config_json"], json.dumps(research_config))

    # Secrets — prompt once each
    print()
    redis_auth = input("  Redis AUTH token (Enter to skip): ").strip()
    if redis_auth:
        store_in_ssm(SSM_PATHS["redis_auth_token"], redis_auth, is_secure=True)
    else:
        warn("Skipping Redis AUTH token")

    openai_key = input("  OpenAI API key (Enter to skip):   ").strip()
    if openai_key:
        store_in_ssm(SSM_PATHS["openai_api_key"],        openai_key, is_secure=True)
        store_in_ssm(SSM_PATHS["critic_openai_api_key"], openai_key, is_secure=True)
    else:
        warn("Skipping OpenAI API key — set manually later")



# STEP 9 — LAMBDA VPC CONFIG

def setup_lambda_vpc(vpc_info: Dict, sg_info: Dict, queue_urls: Dict, redis_info: Dict):
    """
    Attach the aggregator Lambda to the VPC so it can reach ElastiCache,
    and set all required environment variables.
    Skips gracefully if the function doesn't exist yet.
    """
    section("Step 9 — Lambda VPC Config & Environment Variables")

    try:
        config = lambda_.get_function_configuration(FunctionName=LAMBDA_FUNCTION)
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            warn(f"Lambda '{LAMBDA_FUNCTION}' not deployed yet — skipping VPC config.")
            warn("Deploy the Lambda first, then re-run this script.")
            return
        raise

    current_vpc = config.get("VpcConfig", {}).get("VpcId", "")
    target_vpc  = vpc_info["vpc_id"]

    if current_vpc == target_vpc:
        skip(f"Lambda already in VPC {target_vpc}")
    else:
        log(f"Attaching Lambda to VPC {target_vpc}")
        lambda_.update_function_configuration(
            FunctionName=LAMBDA_FUNCTION,
            VpcConfig={
                "SubnetIds":        vpc_info["subnet_ids"],
                "SecurityGroupIds": [sg_info["lambda_sg_id"]],
            }
        )
        # Wait for update to complete
        log("Waiting for Lambda VPC update to complete...")
        for _ in range(20):
            time.sleep(3)
            state = lambda_.get_function_configuration(
                FunctionName=LAMBDA_FUNCTION
            ).get("LastUpdateStatus", "")
            if state == "Successful":
                break
        ok(f"Lambda attached to VPC {target_vpc}")

    # Set environment variables
    log("Updating Lambda environment variables")
    lambda_.update_function_configuration(
        FunctionName=LAMBDA_FUNCTION,
        Environment={
            "Variables": {
                "REDIS_HOST":            redis_info["host"],
                "REDIS_PORT":            redis_info["port"],
                "CRITIC_TASKS_QUEUE_URL": queue_urls["critic_tasks"],
                "EXPECTED_RESULTS":      "3",
                "NUM_CRITIC_SLOTS":      "3",
            }
        }
    )
    ok("Lambda environment variables updated")



# STEP 10 — ESM

def setup_esm(queue_urls: Dict):
    section("Step 10 — Event Source Mapping")

    queue_url = queue_urls["research_results"]
    queue_arn = sqs.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]

    try:
        lambda_.get_function(FunctionName=LAMBDA_FUNCTION)
    except ClientError:
        warn(f"Lambda '{LAMBDA_FUNCTION}' not found — skipping ESM.")
        warn("Deploy Lambda first then re-run.")
        return

    # Check for existing ESM
    paginator = lambda_.get_paginator("list_event_source_mappings")
    for page in paginator.paginate(FunctionName=LAMBDA_FUNCTION):
        for mapping in page["EventSourceMappings"]:
            if mapping["EventSourceArn"] == queue_arn:
                skip(f"ESM already exists — UUID: {mapping['UUID']} | State: {mapping['State']}")
                return

    log(f"Creating ESM: {queue_arn} → {LAMBDA_FUNCTION}")
    try:
        resp = lambda_.create_event_source_mapping(
            EventSourceArn=queue_arn,
            FunctionName=LAMBDA_FUNCTION,
            BatchSize=BATCH_SIZE,
            FunctionResponseTypes=["ReportBatchItemFailures"],
            ScalingConfig={"MaximumConcurrency": MAX_CONCURRENCY},
            Enabled=True,
        )
        uuid = resp["UUID"]
        ok(f"ESM created: {uuid}")

        # Poll until Enabled
        log("Waiting for ESM to become active...")
        for _ in range(20):
            time.sleep(3)
            state = lambda_.get_event_source_mapping(UUID=uuid)["State"]
            print(f"    State: {state}")
            if state == "Enabled":
                ok("ESM active")
                return
        warn("ESM not yet active — check Lambda console → Triggers")

    except ClientError as e:
        fail(f"ESM creation failed: {e}")



# SAVE LOCAL CONFIG

def save_local_config(queue_urls: Dict, redis_info: Dict, vpc_info: Dict, sg_info: Dict):
    config = {
        "region":     REGION,
        "account_id": ACCOUNT_ID,
        "vpc":        vpc_info,
        "security_groups": sg_info,
        "queues":     queue_urls,
        "bucket":     BUCKET,
        "ssm_paths":  SSM_PATHS,
        "redis": {
            "host": redis_info["host"],
            "port": int(redis_info["port"]),
        }
    }
    with open("debate_config.json", "w") as f:
        json.dump(config, f, indent=2)
    ok("Saved debate_config.json")



# MAIN

def main():
    print("\n" + "="*60)
    print("  Infrastructure Setup")
    print("="*60)

    check_credentials()

    vpc_info  = setup_vpc()
    sg_info   = setup_security_groups(vpc_info["vpc_id"])

    setup_vpc_endpoints(
        vpc_info["vpc_id"],
        vpc_info["subnet_ids"],
        sg_info["lambda_sg_id"],
        vpc_info["route_table_id"],
    )

    queue_urls = setup_queues()
    setup_s3()

    redis_info = setup_elasticache(
        vpc_info["subnet_ids"],
        sg_info["elasticache_sg_id"],
    )

    setup_ssm(queue_urls, redis_info)
    save_local_config(queue_urls, redis_info, vpc_info, sg_info)

    setup_lambda_vpc(vpc_info, sg_info, queue_urls, redis_info)
    # setup_esm(queue_urls)

    print("\n" + "="*60)
    print(" SETUP COMPLETE")
    print("="*60)
    print(f"\n  VPC            : {vpc_info['vpc_id']}")
    print(f"  Subnets        : {vpc_info['subnet_ids']}")
    print(f"  Lambda SG      : {sg_info['lambda_sg_id']}")
    print(f"  ElastiCache SG : {sg_info['elasticache_sg_id']}")
    print(f"  Redis          : {redis_info['host']}:{redis_info['port']}")
    print(f"  Config saved   : debate_config.json")
    print()
    print("  Shell scripts — update these values:")
    print(f"    SECURITY_GROUP_ID = {sg_info['lambda_sg_id']}")
    print(f"    VPC subnets       = {vpc_info['subnet_ids']}")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()