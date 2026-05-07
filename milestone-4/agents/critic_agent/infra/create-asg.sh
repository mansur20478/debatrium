#!/bin/bash
# create-critic-asg.sh - Uses the debate-vpc created by setup.py

set -e

ASG_NAME="critic-agent-asg"
LAUNCH_TEMPLATE_NAME="critic-agent-template"
DESIRED_CAPACITY=2
MIN_SIZE=2
MAX_SIZE=2
REGION=$(aws configure get region 2>/dev/null || echo "us-east-1")
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
VPC_NAME="debate-vpc-${ACCOUNT_ID: -6}"

echo "Region     : $REGION"
echo "Account    : $ACCOUNT_ID"
echo "VPC name   : $VPC_NAME"


# ─────────────────────────────────────────────────────────────
# GET DEBATE VPC
# ─────────────────────────────────────────────────────────────
echo ""
echo "Fetching debate VPC..."

DEBATE_VPC=$(aws ec2 describe-vpcs \
    --region "$REGION" \
    --filters "Name=tag:Name,Values=${VPC_NAME}" \
    --query "Vpcs[0].VpcId" \
    --output text)

if [[ -z "$DEBATE_VPC" || "$DEBATE_VPC" == "None" ]]; then
    echo "ERROR: Debate VPC '${VPC_NAME}' not found."
    echo "       Run setup.py first to create the VPC."
    exit 1
fi

echo "Debate VPC : $DEBATE_VPC"


# ─────────────────────────────────────────────────────────────
# GET SUBNETS IN DEBATE VPC
# ─────────────────────────────────────────────────────────────
echo "Fetching subnets..."

SUBNETS=$(aws ec2 describe-subnets \
    --region "$REGION" \
    --filters "Name=vpc-id,Values=${DEBATE_VPC}" \
              "Name=tag:Name,Values=debate-private-subnet-*" \
    --query "Subnets[*].SubnetId" \
    --output text | tr '\t' ',')

if [[ -z "$SUBNETS" ]]; then
    echo "ERROR: No subnets found in VPC $DEBATE_VPC."
    echo "       Run setup.py first to create the subnets."
    exit 1
fi

echo "Subnets    : $SUBNETS"


# ─────────────────────────────────────────────────────────────
# CHECK LAUNCH TEMPLATE EXISTS
# ─────────────────────────────────────────────────────────────
echo ""
echo "Verifying launch template: ${LAUNCH_TEMPLATE_NAME}..."

TEMPLATE_EXISTS=$(aws ec2 describe-launch-templates \
    --region "$REGION" \
    --launch-template-names "$LAUNCH_TEMPLATE_NAME" \
    --query "LaunchTemplates[0].LaunchTemplateName" \
    --output text 2>/dev/null || echo "None")

if [[ "$TEMPLATE_EXISTS" == "None" || -z "$TEMPLATE_EXISTS" ]]; then
    echo "ERROR: Launch template '${LAUNCH_TEMPLATE_NAME}' not found."
    echo "       Run create-critic-launch-template.sh first."
    exit 1
fi

echo "Template   : $TEMPLATE_EXISTS (found)"


# ─────────────────────────────────────────────────────────────
# DELETE EXISTING ASG IF IT EXISTS
# ─────────────────────────────────────────────────────────────
EXISTING_ASG=$(aws autoscaling describe-auto-scaling-groups \
    --region "$REGION" \
    --auto-scaling-group-names "${ASG_NAME}" \
    --query "length(AutoScalingGroups)" \
    --output text 2>/dev/null || echo "0")

if [[ "$EXISTING_ASG" -gt "0" ]]; then
    echo ""
    echo "ASG '${ASG_NAME}' already exists — skipping creation."
    echo "To recreate it, delete it first:"
    echo "  aws autoscaling delete-auto-scaling-group --auto-scaling-group-name ${ASG_NAME} --force-delete"
else

    # ─────────────────────────────────────────────────────────
    # CREATE AUTO SCALING GROUP
    # ─────────────────────────────────────────────────────────
    echo ""
    echo "Creating Auto Scaling Group: ${ASG_NAME}..."

    aws autoscaling create-auto-scaling-group \
        --region "$REGION" \
        --auto-scaling-group-name "${ASG_NAME}" \
        --launch-template "LaunchTemplateName=${LAUNCH_TEMPLATE_NAME},Version=\$Latest" \
        --min-size ${MIN_SIZE} \
        --max-size ${MAX_SIZE} \
        --desired-capacity ${DESIRED_CAPACITY} \
        --vpc-zone-identifier "${SUBNETS}" \
        --health-check-type EC2 \
        --health-check-grace-period 300 \
        --tags \
            Key=Name,Value=CriticAgent,PropagateAtLaunch=true \
            Key=Environment,Value=Lab,PropagateAtLaunch=true \
            Key=Role,Value=critic-agent,PropagateAtLaunch=true \
            Key=VPC,Value="${DEBATE_VPC}",PropagateAtLaunch=true

    echo "✓ Auto Scaling Group created: ${ASG_NAME}"
fi


# ─────────────────────────────────────────────────────────────
# WAIT FOR INSTANCES
# ─────────────────────────────────────────────────────────────
echo ""
echo "Waiting for instances to reach InService state..."

for i in $(seq 1 24); do
    IN_SERVICE=$(aws autoscaling describe-auto-scaling-groups \
        --region "$REGION" \
        --auto-scaling-group-names "${ASG_NAME}" \
        --query "length(AutoScalingGroups[0].Instances[?LifecycleState=='InService'])" \
        --output text 2>/dev/null || echo "0")

    echo "  [${i}/24] InService: ${IN_SERVICE}/${DESIRED_CAPACITY}  (checking every 15s)"

    if [[ "$IN_SERVICE" -ge "$DESIRED_CAPACITY" ]]; then
        echo "✓ All instances InService."
        break
    fi

    if [[ "$i" -eq 24 ]]; then
        echo "WARNING: Timed out after 6 minutes. Instances may still be starting."
        echo "         Check EC2 console for instance status."
    fi

    sleep 15
done


# ─────────────────────────────────────────────────────────────
# SHOW FINAL STATUS
# ─────────────────────────────────────────────────────────────
echo ""
echo "Current instances:"
aws autoscaling describe-auto-scaling-groups \
    --region "$REGION" \
    --auto-scaling-group-names "${ASG_NAME}" \
    --query "AutoScalingGroups[0].Instances[*].[InstanceId,LifecycleState,HealthStatus]" \
    --output table

echo ""
echo "============================================================"
echo "  Critic ASG Setup Complete"
echo "============================================================"
echo "  ASG name   : ${ASG_NAME}"
echo "  VPC        : ${DEBATE_VPC}"
echo "  Subnets    : ${SUBNETS}"
echo "  Min/Max    : ${MIN_SIZE}/${MAX_SIZE}"
echo "  Desired    : ${DESIRED_CAPACITY}"
echo "  Lenses     : logical | evidence | completeness"
echo "============================================================"