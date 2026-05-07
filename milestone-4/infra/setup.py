

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
VPC_CIDR           = "10.0.0.0/16"
# Public subnets — only the NAT Gateway lives here
PUBLIC_SUBNET_CIDRS  = ["10.0.1.0/24", "10.0.2.0/24"]
# Private subnets — all EC2 agent instances launch here
PRIVATE_SUBNET_CIDRS = ["10.0.11.0/24", "10.0.12.0/24"]
AVAILABILITY_ZONES   = ["us-east-1a", "us-east-1b"]
VPC_NAME             = f"debate-vpc-{ACCOUNT_ID[-6:]}"

# Security groups
LAMBDA_SG_NAME      = f"debate-lambda-sg-{ACCOUNT_ID[-6:]}"
ELASTICACHE_SG_NAME = f"debate-redis-sg-{ACCOUNT_ID[-6:]}"

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

# Lambda functions to attach to VPC
LAMBDA_FUNCTIONS = [
    "aggregator_lambda",
    "critic_aggregator_lambda",
    "judge_aggregator_lambda",
    "orchestrator_lambda",
]

# ESM
BATCH_SIZE      = 10
MAX_CONCURRENCY = 5

# SSM paths
SSM_PATHS = {
    # Research agent
    "tasks_queue_url":            "/research-agent/tasks-queue-url",
    "results_queue_url":          "/research-agent/results-queue-url",
    "openai_api_key":             "/research-agent/openai-api-key",
    "config_json":                "/research-agent/config",
    # Critic agent
    "critic_tasks_queue_url":     "/critic-agent/tasks-queue-url",
    "critic_results_queue_url":   "/critic-agent/results-queue-url",
    "critic_openai_api_key":      "/critic-agent/openai-api-key",
    "critic_config_json":         "/critic-agent/config",
    # Judge agent
    "judge_tasks_queue_url":      "/judge-agent/tasks-queue-url",
    "judge_results_queue_url":    "/judge-agent/results-queue-url",
    "judge_openai_api_key":       "/judge-agent/openai-api-key",
    # Redis
    "redis_host":                 "/debate/redis-host",
    "redis_port":                 "/debate/redis-port",
    "redis_auth_token":           "/debate/redis-auth-token",
    # Orchestrator
    "final_results_queue_url":    "/debate/final-results-queue-url",
    "s3_bucket":                  "/debate/s3-bucket",
}


# CLIENTS

ec2         = boto3.client("ec2",         region_name=REGION)
sqs         = boto3.client("sqs",         region_name=REGION)
s3          = boto3.client("s3",          region_name=REGION)
ssm         = boto3.client("ssm",         region_name=REGION)
elasticache = boto3.client("elasticache", region_name=REGION)
lambda_     = boto3.client("lambda",      region_name=REGION)


# PRINT HELPERS

def log(msg):     print(f"  -> {msg}")
def ok(msg):      print(f"  OK {msg}")
def skip(msg):    print(f"  -- {msg} (already exists)")
def warn(msg):    print(f"  !! {msg}")
def fail(msg):    print(f"  XX {msg}")
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



# STEP 2 — VPC + NAT GATEWAY

def setup_vpc() -> Dict:
    """
    Creates:
      - VPC
      - 2 PUBLIC subnets  (10.0.1.0/24, 10.0.2.0/24) — NAT GW only
      - 2 PRIVATE subnets (10.0.11.0/24, 10.0.12.0/24) — all EC2 agents
      - Internet Gateway  attached to VPC
      - Elastic IP        for NAT Gateway
      - NAT Gateway       in public subnet-1
      - Public route table:  0.0.0.0/0 -> IGW   (associated with public subnets)
      - Private route table: 0.0.0.0/0 -> NAT GW (associated with private subnets)

    EC2 agents in private subnets reach the internet through the shared
    NAT Gateway — no public IP needed on any instance.
    """
    section("Step 2 — VPC & Networking (with NAT Gateway)")

    # ── VPC ───────────────────────────────────────────────────
    vpcs = ec2.describe_vpcs(
        Filters=[{"Name": "tag:Name", "Values": [VPC_NAME]}]
    )["Vpcs"]

    if vpcs:
        vpc_id = vpcs[0]["VpcId"]
        skip(f"VPC: {vpc_id}")
    else:
        log(f"Creating VPC: {VPC_CIDR}")
        vpc    = ec2.create_vpc(CidrBlock=VPC_CIDR)["Vpc"]
        vpc_id = vpc["VpcId"]
        ec2.create_tags(Resources=[vpc_id], Tags=[
            {"Key": "Name",    "Value": VPC_NAME},
            {"Key": "Project", "Value": "Debatrium"},
        ])
        ec2.modify_vpc_attribute(VpcId=vpc_id, EnableDnsSupport={"Value": True})
        ec2.modify_vpc_attribute(VpcId=vpc_id, EnableDnsHostnames={"Value": True})
        ok(f"VPC created: {vpc_id}")

    # ── Public subnets ────────────────────────────────────────
    public_subnet_ids = []
    for i, (cidr, az) in enumerate(zip(PUBLIC_SUBNET_CIDRS, AVAILABILITY_ZONES)):
        existing = ec2.describe_subnets(Filters=[
            {"Name": "vpc-id",    "Values": [vpc_id]},
            {"Name": "cidrBlock", "Values": [cidr]},
        ])["Subnets"]
        if existing:
            public_subnet_ids.append(existing[0]["SubnetId"])
            skip(f"Public subnet {i+1}: {existing[0]['SubnetId']} ({cidr})")
        else:
            log(f"Creating public subnet {i+1}: {cidr} in {az}")
            subnet    = ec2.create_subnet(VpcId=vpc_id, CidrBlock=cidr, AvailabilityZone=az)["Subnet"]
            subnet_id = subnet["SubnetId"]
            ec2.create_tags(Resources=[subnet_id], Tags=[
                {"Key": "Name", "Value": f"debate-public-subnet-{i+1}"},
                {"Key": "Type", "Value": "public"},
            ])
            public_subnet_ids.append(subnet_id)
            ok(f"Public subnet {i+1}: {subnet_id}")

    # ── Private subnets ───────────────────────────────────────
    private_subnet_ids = []
    for i, (cidr, az) in enumerate(zip(PRIVATE_SUBNET_CIDRS, AVAILABILITY_ZONES)):
        existing = ec2.describe_subnets(Filters=[
            {"Name": "vpc-id",    "Values": [vpc_id]},
            {"Name": "cidrBlock", "Values": [cidr]},
        ])["Subnets"]
        if existing:
            private_subnet_ids.append(existing[0]["SubnetId"])
            skip(f"Private subnet {i+1}: {existing[0]['SubnetId']} ({cidr})")
        else:
            log(f"Creating private subnet {i+1}: {cidr} in {az}")
            subnet    = ec2.create_subnet(VpcId=vpc_id, CidrBlock=cidr, AvailabilityZone=az)["Subnet"]
            subnet_id = subnet["SubnetId"]
            ec2.create_tags(Resources=[subnet_id], Tags=[
                {"Key": "Name", "Value": f"debate-private-subnet-{i+1}"},
                {"Key": "Type", "Value": "private"},
            ])
            private_subnet_ids.append(subnet_id)
            ok(f"Private subnet {i+1}: {subnet_id}")

    # ── Internet Gateway ──────────────────────────────────────
    igws = ec2.describe_internet_gateways(
        Filters=[{"Name": "attachment.vpc-id", "Values": [vpc_id]}]
    )["InternetGateways"]
    if igws:
        igw_id = igws[0]["InternetGatewayId"]
        skip(f"IGW: {igw_id}")
    else:
        log("Creating Internet Gateway")
        igw    = ec2.create_internet_gateway()["InternetGateway"]
        igw_id = igw["InternetGatewayId"]
        ec2.attach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)
        ec2.create_tags(Resources=[igw_id], Tags=[
            {"Key": "Name", "Value": f"debate-igw-{ACCOUNT_ID[-6:]}"},
        ])
        ok(f"IGW created and attached: {igw_id}")

    # ── Elastic IP for NAT Gateway ────────────────────────────
    eips = ec2.describe_addresses(
        Filters=[{"Name": "tag:Name", "Values": ["debate-nat-eip"]}]
    )["Addresses"]
    if eips:
        eip_alloc_id = eips[0]["AllocationId"]
        skip(f"Elastic IP: {eip_alloc_id} ({eips[0].get('PublicIp', '')})")
    else:
        log("Allocating Elastic IP for NAT Gateway")
        eip          = ec2.allocate_address(Domain="vpc")
        eip_alloc_id = eip["AllocationId"]
        ec2.create_tags(Resources=[eip_alloc_id], Tags=[
            {"Key": "Name", "Value": "debate-nat-eip"},
        ])
        ok(f"Elastic IP allocated: {eip_alloc_id} ({eip['PublicIp']})")

    # ── NAT Gateway (in public subnet-1) ─────────────────────
    existing_nats = ec2.describe_nat_gateways(
        Filters=[
            {"Name": "vpc-id",              "Values": [vpc_id]},
            {"Name": "state",               "Values": ["available", "pending"]},
            {"Name": "tag:Name",            "Values": ["debate-nat-gw"]},
        ]
    )["NatGateways"]

    if existing_nats:
        nat_gw_id = existing_nats[0]["NatGatewayId"]
        skip(f"NAT Gateway: {nat_gw_id}")
    else:
        log(f"Creating NAT Gateway in public subnet {public_subnet_ids[0]}")
        nat = ec2.create_nat_gateway(
            SubnetId=public_subnet_ids[0],
            AllocationId=eip_alloc_id,
            TagSpecifications=[{
                "ResourceType": "natgateway",
                "Tags": [
                    {"Key": "Name",    "Value": "debate-nat-gw"},
                    {"Key": "Project", "Value": "Debatrium"},
                ],
            }],
        )
        nat_gw_id = nat["NatGateway"]["NatGatewayId"]
        ok(f"NAT Gateway created: {nat_gw_id} — waiting for it to become available...")

        # NAT Gateway takes ~60-90 seconds to become available
        for attempt in range(20):
            time.sleep(10)
            resp   = ec2.describe_nat_gateways(NatGatewayIds=[nat_gw_id])["NatGateways"][0]
            state  = resp["State"]
            print(f"    [{attempt+1}/20] NAT Gateway state: {state}")
            if state == "available":
                ok(f"NAT Gateway ready: {nat_gw_id}")
                break
        else:
            fail("NAT Gateway did not become available in time")
            sys.exit(1)

    # ── Public route table (IGW) ──────────────────────────────
    pub_rts = ec2.describe_route_tables(Filters=[
        {"Name": "vpc-id",   "Values": [vpc_id]},
        {"Name": "tag:Name", "Values": ["debate-public-rt"]},
    ])["RouteTables"]

    if pub_rts:
        pub_rt_id = pub_rts[0]["RouteTableId"]
        skip(f"Public route table: {pub_rt_id}")
    else:
        log("Creating public route table (IGW)")
        rt        = ec2.create_route_table(VpcId=vpc_id)["RouteTable"]
        pub_rt_id = rt["RouteTableId"]
        ec2.create_tags(Resources=[pub_rt_id], Tags=[
            {"Key": "Name", "Value": "debate-public-rt"},
        ])
        ec2.create_route(
            RouteTableId=pub_rt_id,
            DestinationCidrBlock="0.0.0.0/0",
            GatewayId=igw_id,
        )
        ok(f"Public route table created: {pub_rt_id}")

    # Associate public route table with public subnets
    for subnet_id in public_subnet_ids:
        assocs = ec2.describe_route_tables(
            Filters=[{"Name": "association.subnet-id", "Values": [subnet_id]}]
        )["RouteTables"]
        if not assocs:
            ec2.associate_route_table(RouteTableId=pub_rt_id, SubnetId=subnet_id)
            ok(f"Public RT associated with {subnet_id}")
        else:
            skip(f"Public RT already associated with {subnet_id}")

    # ── Private route table (NAT GW) ──────────────────────────
    priv_rts = ec2.describe_route_tables(Filters=[
        {"Name": "vpc-id",   "Values": [vpc_id]},
        {"Name": "tag:Name", "Values": ["debate-private-rt"]},
    ])["RouteTables"]

    if priv_rts:
        priv_rt_id = priv_rts[0]["RouteTableId"]
        skip(f"Private route table: {priv_rt_id}")
    else:
        log("Creating private route table (NAT GW)")
        rt         = ec2.create_route_table(VpcId=vpc_id)["RouteTable"]
        priv_rt_id = rt["RouteTableId"]
        ec2.create_tags(Resources=[priv_rt_id], Tags=[
            {"Key": "Name", "Value": "debate-private-rt"},
        ])
        ec2.create_route(
            RouteTableId=priv_rt_id,
            DestinationCidrBlock="0.0.0.0/0",
            NatGatewayId=nat_gw_id,
        )
        ok(f"Private route table created: {priv_rt_id}")

    # Associate private route table with private subnets
    for subnet_id in private_subnet_ids:
        assocs = ec2.describe_route_tables(
            Filters=[{"Name": "association.subnet-id", "Values": [subnet_id]}]
        )["RouteTables"]
        if not assocs:
            ec2.associate_route_table(RouteTableId=priv_rt_id, SubnetId=subnet_id)
            ok(f"Private RT associated with {subnet_id}")
        else:
            skip(f"Private RT already associated with {subnet_id}")

    ok(f"VPC ready — {vpc_id}")
    ok(f"  Public  subnets : {public_subnet_ids}")
    ok(f"  Private subnets : {private_subnet_ids}")
    ok(f"  NAT Gateway     : {nat_gw_id} (shared by all agent instances)")

    return {
        "vpc_id":             vpc_id,
        "public_subnet_ids":  public_subnet_ids,
        "private_subnet_ids": private_subnet_ids,
        "public_rt_id":       pub_rt_id,
        "private_rt_id":      priv_rt_id,
        "nat_gw_id":          nat_gw_id,
        # keep subnet_ids as private subnets for backward compat
        # (all agents, lambdas, and ElastiCache go into private subnets)
        "subnet_ids":         private_subnet_ids,
        "route_table_id":     priv_rt_id,
    }



# STEP 3 — SECURITY GROUPS

def setup_security_groups(vpc_id: str) -> Dict:
    section("Step 3 — Security Groups")

    def get_sg(name):
        sgs = ec2.describe_security_groups(Filters=[
            {"Name": "group-name", "Values": [name]},
            {"Name": "vpc-id",     "Values": [vpc_id]},
        ])["SecurityGroups"]
        return sgs[0]["GroupId"] if sgs else None

    # Lambda / agent SG
    lambda_sg_id = get_sg(LAMBDA_SG_NAME)
    if lambda_sg_id:
        skip(f"Lambda SG: {lambda_sg_id}")
    else:
        log(f"Creating Lambda SG: {LAMBDA_SG_NAME}")
        sg           = ec2.create_security_group(
            GroupName=LAMBDA_SG_NAME,
            Description="Debate system - Lambda and EC2 agents",
            VpcId=vpc_id,
        )
        lambda_sg_id = sg["GroupId"]
        ec2.create_tags(Resources=[lambda_sg_id], Tags=[
            {"Key": "Name", "Value": LAMBDA_SG_NAME}
        ])
        ec2.authorize_security_group_ingress(
            GroupId=lambda_sg_id,
            IpPermissions=[
                # Self-referencing — agents talking to each other
                {"IpProtocol": "-1", "UserIdGroupPairs": [{"GroupId": lambda_sg_id}]},
                # HTTPS for VPC endpoint traffic
                {"IpProtocol": "tcp", "FromPort": 443, "ToPort": 443,
                 "IpRanges": [{"CidrIp": "0.0.0.0/0"}]},
            ]
        )
        ok(f"Lambda SG created: {lambda_sg_id}")

    # ElastiCache SG
    redis_sg_id = get_sg(ELASTICACHE_SG_NAME)
    if redis_sg_id:
        skip(f"ElastiCache SG: {redis_sg_id}")
    else:
        log(f"Creating ElastiCache SG: {ELASTICACHE_SG_NAME}")
        sg          = ec2.create_security_group(
            GroupName=ELASTICACHE_SG_NAME,
            Description="Debate system - ElastiCache Redis",
            VpcId=vpc_id,
        )
        redis_sg_id = sg["GroupId"]
        ec2.create_tags(Resources=[redis_sg_id], Tags=[
            {"Key": "Name", "Value": ELASTICACHE_SG_NAME}
        ])
        ec2.authorize_security_group_ingress(
            GroupId=redis_sg_id,
            IpPermissions=[{
                "IpProtocol": "tcp", "FromPort": 6379, "ToPort": 6379,
                "UserIdGroupPairs": [{"GroupId": lambda_sg_id}],
            }]
        )
        ok(f"ElastiCache SG created: {redis_sg_id}")

    return {"lambda_sg_id": lambda_sg_id, "elasticache_sg_id": redis_sg_id}



# STEP 4 — VPC ENDPOINTS

def setup_vpc_endpoints(vpc_id: str, subnet_ids: list, lambda_sg_id: str, route_table_id: str):
    section("Step 4 — VPC Endpoints")

    def endpoint_exists(service_name):
        eps = ec2.describe_vpc_endpoints(Filters=[
            {"Name": "service-name",        "Values": [service_name]},
            {"Name": "vpc-id",              "Values": [vpc_id]},
            {"Name": "vpc-endpoint-state",  "Values": ["available", "pending"]},
        ])["VpcEndpoints"]
        return eps[0]["VpcEndpointId"] if eps else None

    interface_services = {
        "SQS":          f"com.amazonaws.{REGION}.sqs",
        "SSM":          f"com.amazonaws.{REGION}.ssm",
        "SSM Messages": f"com.amazonaws.{REGION}.ssmmessages",
        "Lambda":       f"com.amazonaws.{REGION}.lambda",
    }

    for name, service in interface_services.items():
        ep_id = endpoint_exists(service)
        if ep_id:
            skip(f"{name} endpoint: {ep_id}")
            try:
                ec2.modify_vpc_endpoint(VpcEndpointId=ep_id, PrivateDnsEnabled=True)
            except Exception:
                pass
        else:
            log(f"Creating {name} Interface endpoint")
            ep    = ec2.create_vpc_endpoint(
                VpcEndpointType="Interface",
                VpcId=vpc_id,
                ServiceName=service,
                SubnetIds=subnet_ids,
                SecurityGroupIds=[lambda_sg_id],
                PrivateDnsEnabled=True,
            )
            ep_id = ep["VpcEndpoint"]["VpcEndpointId"]
            ok(f"{name} endpoint created: {ep_id}")

    # S3 Gateway endpoint (free, no SG)
    s3_service = f"com.amazonaws.{REGION}.s3"
    s3_ep_id   = endpoint_exists(s3_service)
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
        "VisibilityTimeout":         "30",    # short — heartbeat keeps messages alive
        "MessageRetentionPeriod":    "345600",
    }
    if dlq_arn:
        attrs["RedrivePolicy"] = json.dumps({
            "deadLetterTargetArn": dlq_arn,
            "maxReceiveCount":     "3",
        })
    return sqs.create_queue(QueueName=name, Attributes=attrs)["QueueUrl"]

def setup_queues() -> Dict[str, str]:
    section("Step 5 — SQS FIFO Queues")
    queue_urls = {}

    # Missing queues for critic results and judge results
    all_queues = {
        **QUEUES,
        "critic_results": f"critic-results-{ACCOUNT_ID[-6:]}.fifo",
        "judge_results":  f"judge-results-{ACCOUNT_ID[-6:]}.fifo",
    }
    all_dlqs = {
        **DLQS,
        "critic_results_dlq": f"critic-results-dlq-{ACCOUNT_ID[-6:]}.fifo",
        "judge_results_dlq":  f"judge-results-dlq-{ACCOUNT_ID[-6:]}.fifo",
    }

    dlq_arns = {}
    for dlq_key, dlq_name in all_dlqs.items():
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
        dlq_arns[dlq_key] = arn

    main_to_dlq = {
        "research_tasks":   "research_tasks_dlq",
        "research_results": "research_results_dlq",
        "critic_tasks":     "critic_tasks_dlq",
        "critic_results":   "critic_results_dlq",
        "judge_tasks":      "judge_tasks_dlq",
        "judge_results":    "judge_results_dlq",
        "final_results":    "final_results_dlq",
    }

    for main_name, queue_name in all_queues.items():
        dlq_key      = main_to_dlq[main_name]
        dlq_arn      = dlq_arns[dlq_key]
        existing_url = get_queue_url(queue_name)
        if existing_url:
            queue_urls[main_name] = existing_url
            skip(f"Queue: {queue_name}")
        else:
            log(f"Creating queue: {queue_name}")
            url                   = create_fifo_queue(queue_name, dlq_arn)
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
            CreateBucketConfiguration={"LocationConstraint": REGION},
        )
    s3.put_public_access_block(
        Bucket=BUCKET,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls":       True,
            "IgnorePublicAcls":      True,
            "BlockPublicPolicy":     True,
            "RestrictPublicBuckets": True,
        },
    )
    ok(f"Bucket created: {BUCKET}")


# ────────────────────────────────────────────────────────────
# STEP 7 — ELASTICACHE

def setup_elasticache(subnet_ids: list, elasticache_sg_id: str) -> Dict:
    section("Step 7 — ElastiCache Redis")

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

    try:
        resp     = elasticache.describe_replication_groups(
            ReplicationGroupId=ELASTICACHE_CLUSTER_ID
        )
        group    = resp["ReplicationGroups"][0]
        status   = group["Status"]
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
            ReplicationGroupId=          ELASTICACHE_CLUSTER_ID,
            ReplicationGroupDescription= "Debate system shared Redis state",
            NumCacheClusters=            1 + ELASTICACHE_NUM_REPLICAS,
            CacheNodeType=               ELASTICACHE_NODE_TYPE,
            Engine=                      ELASTICACHE_ENGINE,
            EngineVersion=               ELASTICACHE_ENGINE_VERSION,
            CacheSubnetGroupName=        ELASTICACHE_SUBNET_GROUP,
            SecurityGroupIds=            [elasticache_sg_id],
            AutomaticFailoverEnabled=    ELASTICACHE_NUM_REPLICAS > 0,
            MultiAZEnabled=              ELASTICACHE_NUM_REPLICAS > 0,
            AtRestEncryptionEnabled=     True,
            TransitEncryptionEnabled=    True,
            Tags=[
                {"Key": "Project", "Value": "Debatrium"},
                {"Key": "Role",    "Value": "shared-state"},
            ],
        )
        ok("Redis cluster creation initiated — this takes ~10 min")

    print("  Polling until available...")
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

    # Research agent
    store_in_ssm(SSM_PATHS["tasks_queue_url"],          queue_urls["research_tasks"])
    store_in_ssm(SSM_PATHS["results_queue_url"],         queue_urls["research_results"])
    # Critic agent
    store_in_ssm(SSM_PATHS["critic_tasks_queue_url"],    queue_urls["critic_tasks"])
    store_in_ssm(SSM_PATHS["critic_results_queue_url"],  queue_urls["critic_results"])
    # Judge agent
    store_in_ssm(SSM_PATHS["judge_tasks_queue_url"],     queue_urls["judge_tasks"])
    store_in_ssm(SSM_PATHS["judge_results_queue_url"],   queue_urls["judge_results"])
    # Shared
    store_in_ssm(SSM_PATHS["redis_host"],                redis_info["host"])
    store_in_ssm(SSM_PATHS["redis_port"],                redis_info["port"])
    store_in_ssm(SSM_PATHS["final_results_queue_url"],   queue_urls["final_results"])
    store_in_ssm(SSM_PATHS["s3_bucket"],                 BUCKET)

    # Full config JSON for agents that need it
    full_config = {
        "region":     REGION,
        "account_id": ACCOUNT_ID,
        "queues":     queue_urls,
        "bucket":     BUCKET,
        "ssm_paths":  SSM_PATHS,
    }
    store_in_ssm(SSM_PATHS["config_json"],        json.dumps(full_config))
    store_in_ssm(SSM_PATHS["critic_config_json"],  json.dumps(full_config))

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
        store_in_ssm(SSM_PATHS["judge_openai_api_key"],  openai_key, is_secure=True)
    else:
        warn("Skipping OpenAI API key — set manually later")



# STEP 9 — LAMBDA VPC CONFIG
# Attaches all four lambdas to the private subnets

def setup_lambda_vpc(vpc_info: Dict, sg_info: Dict, queue_urls: Dict, redis_info: Dict):
    section("Step 9 — Lambda VPC Config & Environment Variables")

    lambda_env = {
        "aggregator_lambda": {
            "REDIS_HOST":             redis_info["host"],
            "REDIS_PORT":             redis_info["port"],
            "CRITIC_TASKS_QUEUE_URL": queue_urls["critic_tasks"],
            "EXPECTED_RESULTS":       "3",
            "NUM_CRITIC_SLOTS":       "3",
        },
        "critic_aggregator_lambda": {
            "REDIS_HOST":            redis_info["host"],
            "REDIS_PORT":            redis_info["port"],
            "JUDGE_TASKS_QUEUE_URL": queue_urls["judge_tasks"],
            "NUM_JUDGE_SLOTS":       "3",
        },
        "judge_aggregator_lambda": {
            "REDIS_HOST":               redis_info["host"],
            "REDIS_PORT":               redis_info["port"],
            "RESEARCH_TASKS_QUEUE_URL": queue_urls["research_tasks"],
            "FINAL_RESULTS_QUEUE_URL":  queue_urls["final_results"],
            "S3_BUCKET":                BUCKET,
            "MAX_ROUNDS":               "3",
            "SCORE_THRESHOLD":          "0.85",
        },
        "orchestrator_lambda": {
            "REDIS_HOST":               redis_info["host"],
            "REDIS_PORT":               redis_info["port"],
            "RESEARCH_TASKS_QUEUE_URL": queue_urls["research_tasks"],
            "S3_BUCKET":                BUCKET,
            "MAX_ROUNDS":               "3",
            "SCORE_THRESHOLD":          "0.85",
        },
    }

    for fn_name in LAMBDA_FUNCTIONS:
        log(f"Configuring Lambda: {fn_name}")
        try:
            config      = lambda_.get_function_configuration(FunctionName=fn_name)
            current_vpc = config.get("VpcConfig", {}).get("VpcId", "")
            target_vpc  = vpc_info["vpc_id"]

            if current_vpc == target_vpc:
                skip(f"{fn_name} already in VPC {target_vpc}")
            else:
                log(f"Attaching {fn_name} to VPC {target_vpc}")
                lambda_.update_function_configuration(
                    FunctionName=fn_name,
                    VpcConfig={
                        "SubnetIds":        vpc_info["private_subnet_ids"],
                        "SecurityGroupIds": [sg_info["lambda_sg_id"]],
                    },
                )
                for _ in range(20):
                    time.sleep(3)
                    state = lambda_.get_function_configuration(
                        FunctionName=fn_name
                    ).get("LastUpdateStatus", "")
                    if state == "Successful":
                        break
                ok(f"{fn_name} attached to VPC")

            lambda_.update_function_configuration(
                FunctionName=fn_name,
                Environment={"Variables": lambda_env[fn_name]},
            )
            ok(f"{fn_name} environment variables updated")

        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                warn(f"Lambda '{fn_name}' not deployed yet — skipping")
            else:
                raise



# STEP 10 — ESM (Event Source Mappings)
# Wires each results queue to its aggregator lambda

def setup_esm(queue_urls: Dict):
    section("Step 10 — Event Source Mappings")

    esm_pairs = [
        ("research_results", "aggregator_lambda"),
        ("critic_results",   "critic_aggregator_lambda"),
        ("judge_results",    "judge_aggregator_lambda"),
    ]

    for queue_key, fn_name in esm_pairs:
        log(f"ESM: {queue_key} -> {fn_name}")
        queue_url = queue_urls[queue_key]
        queue_arn = sqs.get_queue_attributes(
            QueueUrl=queue_url, AttributeNames=["QueueArn"]
        )["Attributes"]["QueueArn"]

        try:
            lambda_.get_function(FunctionName=fn_name)
        except ClientError:
            warn(f"Lambda '{fn_name}' not found — skipping ESM")
            continue

        paginator = lambda_.get_paginator("list_event_source_mappings")
        exists    = False
        for page in paginator.paginate(FunctionName=fn_name):
            for mapping in page["EventSourceMappings"]:
                if mapping["EventSourceArn"] == queue_arn:
                    skip(f"ESM {queue_key} -> {fn_name}: {mapping['UUID']}")
                    exists = True
                    break

        if exists:
            continue

        try:
            resp = lambda_.create_event_source_mapping(
                EventSourceArn=queue_arn,
                FunctionName=fn_name,
                BatchSize=BATCH_SIZE,
                FunctionResponseTypes=["ReportBatchItemFailures"],
                ScalingConfig={"MaximumConcurrency": MAX_CONCURRENCY},
                Enabled=True,
            )
            uuid = resp["UUID"]
            ok(f"ESM created: {uuid}")

            for _ in range(20):
                time.sleep(3)
                state = lambda_.get_event_source_mapping(UUID=uuid)["State"]
                if state == "Enabled":
                    ok(f"ESM active: {queue_key} -> {fn_name}")
                    break
        except ClientError as e:
            fail(f"ESM creation failed: {e}")



# SAVE LOCAL CONFIG

def save_local_config(queue_urls: Dict, redis_info: Dict, vpc_info: Dict, sg_info: Dict):
    config = {
        "region":          REGION,
        "account_id":      ACCOUNT_ID,
        "vpc":             vpc_info,
        "security_groups": sg_info,
        "queues":          queue_urls,
        "bucket":          BUCKET,
        "ssm_paths":       SSM_PATHS,
        "redis": {
            "host": redis_info["host"],
            "port": int(redis_info["port"]),
        },
    }
    with open("debate_config.json", "w") as f:
        json.dump(config, f, indent=2)
    ok("Saved debate_config.json")



# MAIN

def main():
    print("\n" + "="*60)
    print("  DISTRIBUTED DEBATE SYSTEM — Full Infrastructure Setup")
    print("="*60)

    check_credentials()

    vpc_info = setup_vpc()
    sg_info  = setup_security_groups(vpc_info["vpc_id"])

    setup_vpc_endpoints(
        vpc_info["vpc_id"],
        vpc_info["private_subnet_ids"],   # endpoints go in private subnets
        sg_info["lambda_sg_id"],
        vpc_info["private_rt_id"],        # S3 gateway goes on private RT
    )

    queue_urls = setup_queues()
    setup_s3()

    redis_info = setup_elasticache(
        vpc_info["private_subnet_ids"],   # Redis in private subnets
        sg_info["elasticache_sg_id"],
    )

    setup_ssm(queue_urls, redis_info)
    save_local_config(queue_urls, redis_info, vpc_info, sg_info)

    # setup_lambda_vpc(vpc_info, sg_info, queue_urls, redis_info)
    # setup_esm(queue_urls)

    print("\n" + "="*60)
    print("  SETUP COMPLETE")
    print("="*60)
    print(f"\n  VPC              : {vpc_info['vpc_id']}")
    print(f"  Public  subnets  : {vpc_info['public_subnet_ids']}")
    print(f"  Private subnets  : {vpc_info['private_subnet_ids']}")
    print(f"  NAT Gateway      : {vpc_info['nat_gw_id']}  (one shared public IP)")
    print(f"  Lambda SG        : {sg_info['lambda_sg_id']}")
    print(f"  ElastiCache SG   : {sg_info['elasticache_sg_id']}")
    print(f"  Redis            : {redis_info['host']}:{redis_info['port']}")
    print(f"  Config saved     : debate_config.json")
    print()
    print("  Launch template scripts — use these subnet values:")
    print(f"    Private subnets (agents) : {vpc_info['private_subnet_ids']}")
    print(f"    Security group           : {sg_info['lambda_sg_id']}")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()