# Complete Guide: RDS PostgreSQL to MSK CDC with Debezium

A comprehensive guide for setting up Change Data Capture (CDC) from Amazon RDS PostgreSQL to Amazon MSK using Debezium, with MongoDB Atlas Stream Processing integration.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Prerequisites](#2-prerequisites)
3. [Part 1: Atlas Stream Processing IP Configuration](#part-1-atlas-stream-processing-ip-configuration)
4. [Part 2: AWS Secrets Manager Setup](#part-2-aws-secrets-manager-setup)
5. [Part 3: Security Groups Configuration](#part-3-security-groups-configuration)
6. [Part 4: MSK Cluster Configuration](#part-4-msk-cluster-configuration)
7. [Part 5: MSK Cluster Deployment](#part-5-msk-cluster-deployment)
8. [Part 6: EC2 Admin Instance Setup](#part-6-ec2-admin-instance-setup)
9. [Part 7: Kafka Authentication & ACLs](#part-7-kafka-authentication--acls)
10. [Part 8: RDS PostgreSQL CDC Configuration](#part-8-rds-postgresql-cdc-configuration)
11. [Part 9: Debezium Connector Setup](#part-9-debezium-connector-setup)
12. [Part 10: MSK Connect Configuration](#part-10-msk-connect-configuration)
13. [Verification & Testing](#verification--testing)
14. [Kafka Concepts Deep Dive](#kafka-concepts-deep-dive)
15. [Troubleshooting](#troubleshooting)
16. [Best Practices](#best-practices)
17. [Quick Reference](#quick-reference)

---

## 1. Architecture Overview

### High-Level Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  RDS PostgreSQL │────▶│    Debezium     │────▶│   Amazon MSK    │────▶│  MongoDB Atlas  │
│   (Source DB)   │ CDC │  (MSK Connect)  │     │  (Kafka Broker) │     │ Stream Process  │
└─────────────────┘     └─────────────────┘     └─────────────────┘     └─────────────────┘
```

### Detailed Component Diagram

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                                    AWS VPC                                        │
│                              CIDR: <private-ip>/16                                 │
│  ┌─────────────────────────────────────────────────────────────────────────────┐ │
│  │                           Private/Public Subnets                            │ │
│  │                                                                             │ │
│  │  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐        │ │
│  │  │  RDS PostgreSQL │    │   MSK Cluster   │    │  EC2 Admin      │        │ │
│  │  │                 │    │                 │    │  Instance       │        │ │
│  │  │  • CDC enabled  │    │  • 2 brokers    │    │                 │        │ │
│  │  │  • WAL logical  │    │  • SASL/SCRAM   │    │  • Kafka tools  │        │ │
│  │  │  • Debezium user│    │  • IAM auth     │    │  • IAM role     │        │ │
│  │  └────────┬────────┘    └────────┬────────┘    └────────┬────────┘        │ │
│  │           │                      │                      │                  │ │
│  │           └──────────────────────┼──────────────────────┘                  │ │
│  │                                  │                                          │ │
│  │                    ┌─────────────┴─────────────┐                           │ │
│  │                    │      MSK Connect          │                           │ │
│  │                    │   (Debezium Connector)    │                           │ │
│  │                    └───────────────────────────┘                           │ │
│  └─────────────────────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        │ VPC Peering
                                        ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                         MongoDB Atlas                                             │
│                    CIDR: <private-ip>/21                                        │
│                                                                                   │
│                    ┌─────────────────────────┐                                   │
│                    │   Stream Processing     │                                   │
│                    │   Instance              │                                   │
│                    └─────────────────────────┘                                   │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### Data Flow

1. **Source**: RDS PostgreSQL captures changes via logical replication (WAL)
2. **Capture**: Debezium connector reads changes from PostgreSQL replication slot
3. **Transport**: Changes are published to MSK Kafka topics
4. **Process**: MongoDB Atlas Stream Processing consumes from Kafka topics

---

## 2. Prerequisites

### AWS Resources Required

| Resource | Details | Purpose |
|----------|---------|---------|
| **VPC** | At least 2 public + 2 private subnets | Network isolation |
| **KMS CMK** | Customer Managed Key | Encrypt Secrets Manager secrets |
| **EC2 Instance** | In same VPC, Docker installed | Kafka admin operations |
| **RDS PostgreSQL** | PostgreSQL 14+ | Source database |
| **IAM Permissions** | EC2, MSK, Secrets Manager, S3 | Service access |

### Configuration Values Used in This Guide

```bash
# VPC Configuration
VPC_ID="<vpc-id>"                    # DEMO Peering VPC
VPC_CIDR="<private-ip>/16"
ATLAS_CIDR="<private-ip>/21"

# KMS Configuration
KMS_CMK_ARN="arn:aws:kms:ap-southeast-1:<aws-account-id>:key/350e4194-f794-4f85-9a58-85e88d9c4122"

# Region
AWS_REGION="ap-southeast-1"

# Subnets
SUBNET_1="<subnet-id>"
SUBNET_2="<subnet-id>"
```

### MongoDB Atlas Configuration

- VPC Peering configured between AWS VPC and Atlas
- Atlas CIDR Block: `<private-ip>/21`
- Stream Processing instance deployed in peered region

---

## Part 1: Atlas Stream Processing IP Configuration

### Understanding Atlas IP Addresses

Atlas Stream Processing sends outbound requests from a specific set of IP addresses. You need to allowlist these IPs on your firewall for incoming requests.

### Get Atlas Control Plane IP Addresses

```bash
curl -H 'Accept: application/vnd.atlas.2023-11-15+json' -s \
  'https://cloud.mongodb.com/api/atlas/v2/unauth/controlPlaneIPAddresses'
```

This returns IP addresses grouped by provider and region. Identify all outbound IP addresses for your provider-region pair and add them to your external data source's access list.

### Connection Options

When configuring a connection to an external streaming data source, you can choose:

1. **Public IP addresses** - Direct internet access
2. **VPC Peering connection** - Private network connectivity (recommended for production)

For more details, see [Add a Connection to the Connection Registry](https://www.mongodb.com/docs/atlas/atlas-sp/manage-connection-registry/).

---

## Part 2: AWS Secrets Manager Setup

### Why Secrets Manager?

AWS Secrets Manager provides:
- **No hardcoded secrets** - Keeps codebases clean
- **IAM-based access control** - Only specific services/roles can retrieve secrets
- **Automatic rotation** - For RDS, Redshift, etc.
- **Audit trail** - Track who accessed what and when

> **Important**: MSK only supports Secrets Manager secrets encrypted with a KMS CMK, not AWS-managed KMS keys.

### Step 2.1: Create Project Directory

```bash
# Create a project folder
mkdir -p <local-path>
cd ~/msk-cdc-project
```

### Step 2.2: Create Resource Policy File

```bash
cat > secret_resource_policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "kafka.amazonaws.com"
      },
      "Action": "secretsmanager:GetSecretValue",
      "Resource": "*"
    }
  ]
}
EOF
```

### Step 2.3: Create Secret with KMS Encryption

```bash
# Set your KMS CMK ARN
KMS_CMK_ARN="arn:aws:kms:ap-southeast-1:<aws-account-id>:key/350e4194-f794-4f85-9a58-85e88d9c4122"

# Create the secret
aws secretsmanager create-secret \
  --name AmazonMSK_paul_creds1 \
  --description "MSK SASL/SCRAM credentials" \
  --secret-string '{"username":"pcleene","password":"<password>"}' \
  --kms-key-id $KMS_CMK_ARN \
  --region ap-southeast-1
```

### Step 2.4: Get Secret ARN and Apply Resource Policy

```bash
# Get the secret ARN
SECRET_ARN=$(aws secretsmanager describe-secret \
  --secret-id AmazonMSK_paul_creds1 \
  --region ap-southeast-1 | jq -r '.ARN')

echo "Secret ARN: $SECRET_ARN"

# Apply the resource policy
aws secretsmanager put-resource-policy \
  --secret-id AmazonMSK_paul_creds1 \
  --resource-policy file://secret_resource_policy.json \
  --region ap-southeast-1
```

---

## Part 3: Security Groups Configuration

### Step 3.1: Create MSK Security Group

```bash
# Create security group for MSK
aws ec2 create-security-group \
    --group-name MSK-paul \
    --description "Security group for MSK paulC" \
    --vpc-id <vpc-id> \
    --region ap-southeast-1

# Get the security group ID
SG_ID=$(aws ec2 describe-security-groups \
    --filters Name=group-name,Values=MSK-paul \
    --query "SecurityGroups[*].GroupId" \
    --output text \
    --region ap-southeast-1)

echo "Security Group ID: $SG_ID"
```

### Step 3.2: Configure Ingress Rules

```bash
# Rule 1: Internal VPC access (all protocols)
aws ec2 authorize-security-group-ingress \
    --group-id $SG_ID \
    --protocol -1 \
    --port -1 \
    --cidr <private-ip>/16 \
    --region ap-southeast-1

# Rule 2: MSK Bootstrap Servers - TLS Connection (public access)
aws ec2 authorize-security-group-ingress \
    --group-id $SG_ID \
    --protocol tcp \
    --port 9196 \
    --cidr 0.0.0.0/0 \
    --region ap-southeast-1

# Rule 3: Atlas VPC Peering access
aws ec2 authorize-security-group-ingress \
    --group-id $SG_ID \
    --protocol -1 \
    --port -1 \
    --cidr <private-ip>/21 \
    --region ap-southeast-1
```

### Security Group Rules Summary

| Rule | Protocol | Port | Source | Purpose |
|------|----------|------|--------|---------|
| 1 | All | All | <private-ip>/16 | Internal VPC traffic |
| 2 | TCP | 9196 | 0.0.0.0/0 | Public SASL/SCRAM access |
| 3 | All | All | <private-ip>/21 | Atlas VPC Peering |

---

## Part 4: MSK Cluster Configuration

### Step 4.1: Create Cluster Configuration File

```bash
cat > cluster-config.txt << 'EOF'
allow.everyone.if.no.acl.found=false
auto.create.topics.enable=true
default.replication.factor=1
min.insync.replicas=1
num.io.threads=8
num.network.threads=5
num.partitions=1
num.replica.fetchers=2
replica.lag.time.max.ms=30000
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
socket.send.buffer.bytes=102400
unclean.leader.election.enable=true
zookeeper.session.timeout.ms=18000
EOF
```

### Configuration Parameters Explained

| Parameter | Value | Description |
|-----------|-------|-------------|
| `allow.everyone.if.no.acl.found` | false | Deny access if no ACL exists (security-first) |
| `auto.create.topics.enable` | true | Allow automatic topic creation |
| `default.replication.factor` | 1 | Number of replicas per partition |
| `min.insync.replicas` | 1 | Minimum replicas that must acknowledge writes |
| `num.partitions` | 1 | Default partitions for new topics |

### Step 4.2: Create MSK Configuration

```bash
aws kafka create-configuration \
    --name "paul-configuration" \
    --description "Topic autocreation enabled; Replication Factor 1" \
    --kafka-versions "3.6.0" \
    --server-properties fileb://cluster-config.txt \
    --region ap-southeast-1

# Get configuration ARN
CONFIG_ARN=$(aws kafka list-configurations \
    --query "Configurations[?Name=='paul-configuration'].Arn" \
    --output text \
    --region ap-southeast-1)

echo "Configuration ARN: $CONFIG_ARN"
```

---

## Part 5: MSK Cluster Deployment

### Step 5.1: Create MSK Cluster

```bash
# Ensure variables are set
SG_ID=$(aws ec2 describe-security-groups \
    --filters Name=group-name,Values=MSK-paul \
    --query "SecurityGroups[*].GroupId" \
    --output text \
    --region ap-southeast-1)

CONFIG_ARN=$(aws kafka list-configurations \
    --query "Configurations[?Name=='paul-configuration'].Arn" \
    --output text \
    --region ap-southeast-1)

# Create the cluster
aws kafka create-cluster \
    --cluster-name demo-cluster \
    --kafka-version 3.6.0 \
    --number-of-broker-nodes 2 \
    --broker-node-group-info InstanceType=kafka.t3.small,ClientSubnets=<subnet-id>,<subnet-id>,SecurityGroups=$SG_ID,StorageInfo={EbsStorageInfo={VolumeSize=100}} \
    --client-authentication Unauthenticated={Enabled="False"},Sasl={Scram={Enabled="True"}} \
    --encryption-info EncryptionInTransit={ClientBroker="TLS"} \
    --tags owner=paul.cleenewerck,purpose=Lab,no-reap=true \
    --configuration-info Arn=$CONFIG_ARN,Revision=1 \
    --region ap-southeast-1
```

### Step 5.2: Get Cluster ARN and Associate Secret

```bash
# Wait for cluster to be ACTIVE (this can take 15-20 minutes)
# Check status periodically:
aws kafka list-clusters \
    --query "ClusterInfoList[?ClusterName=='demo-cluster'].State" \
    --output text \
    --region ap-southeast-1

# Once ACTIVE, get cluster ARN
CLUSTER_ARN=$(aws kafka list-clusters \
    --query "ClusterInfoList[?ClusterName=='demo-cluster'].ClusterArn" \
    --output text \
    --region ap-southeast-1)

echo "Cluster ARN: $CLUSTER_ARN"

# Associate SCRAM secret with cluster
SECRET_ARN=$(aws secretsmanager describe-secret \
    --secret-id AmazonMSK_paul_creds1 \
    --region ap-southeast-1 | jq -r '.ARN')

aws kafka batch-associate-scram-secret \
    --cluster-arn $CLUSTER_ARN \
    --secret-arn-list $SECRET_ARN \
    --region ap-southeast-1
```

### Step 5.3: Enable Public Access

```bash
# Get current cluster version
CLUSTER_VERSION=$(aws kafka describe-cluster \
    --cluster-arn $CLUSTER_ARN \
    --query "ClusterInfo.CurrentVersion" \
    --output text \
    --region ap-southeast-1)

# Enable public access
aws kafka update-connectivity \
    --cluster-arn $CLUSTER_ARN \
    --current-version $CLUSTER_VERSION \
    --connectivity-info '{"PublicAccess": {"Type": "SERVICE_PROVIDED_EIPS"}}' \
    --region ap-southeast-1
```

---

## Part 6: EC2 Admin Instance Setup

### Understanding the Architecture

```
┌─────────────────┐         ┌──────────────────┐
│  Your EC2       │ ------> │  MSK Cluster     │
│  (Admin tools)  │  talks  │  (Kafka brokers) │
│                 │   to    │  (managed by AWS)│
└─────────────────┘         └──────────────────┘
```

> **Why EC2?** You can't SSH into MSK brokers directly. You need a separate EC2 instance to run Kafka admin commands.

### Step 6.1: Launch EC2 Instance

```bash
# Create EC2 instance in the same VPC as MSK
aws ec2 run-instances \
    --image-id ami-0d4e5f6071829304a \
    --instance-type t3.micro \
    --key-name YOUR_KEY_PAIR_NAME \
    --security-group-ids $SG_ID \
    --subnet-id <subnet-id> \
    --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=MSK-Admin-Host}]' \
    --region ap-southeast-1
```

> **Replace** `YOUR_KEY_PAIR_NAME` with your actual EC2 key pair name.

### Step 6.2: Connect to EC2 Instance

```bash
# Get the public IP
INSTANCE_IP=$(aws ec2 describe-instances \
    --filters "Name=tag:Name,Values=MSK-Admin-Host" "Name=instance-state-name,Values=running" \
    --query "Reservations[0].Instances[0].PublicIpAddress" \
    --output text \
    --region ap-southeast-1)

echo "SSH to: $INSTANCE_IP"

# SSH in (Amazon Linux)
ssh -i ~/.ssh/your-key.pem ec2-user@$INSTANCE_IP

# For Ubuntu:
ssh -i "<local-path> keys copy/demo_key.pem" ubuntu@<public-ip>
```

### Step 6.3: Install Kafka Tools on EC2

Execute inside the EC2 instance:

```bash
# Update system
sudo apt update && sudo apt upgrade -y  # Ubuntu
# OR
sudo yum update -y  # Amazon Linux

# Install Java
sudo apt install default-jdk -y  # Ubuntu
# OR
sudo yum install java-11-amazon-corretto -y  # Amazon Linux

# Download Kafka
wget https://archive.apache.org/dist/kafka/3.6.0/kafka_2.13-3.6.0.tgz
tar -xvf kafka_2.13-3.6.0.tgz

# Verify installation
ls ~/kafka_2.13-3.6.0/bin/
```

### Step 6.4: Download AWS MSK IAM Auth Library

```bash
cd ~/kafka_2.13-3.6.0/libs/
wget https://github.com/aws/aws-msk-iam-auth/releases/download/v1.1.6/aws-msk-iam-auth-1.1.6-all.jar
ls -lh aws-msk-iam-auth-1.1.6-all.jar
```

---

## Part 7: Kafka Authentication & ACLs

### Understanding the Chicken-and-Egg Problem

When `allow.everyone.if.no.acl.found=false`:

1. Cluster has NO ACLs initially
2. User tries to create ACLs
3. Kafka checks: "Does this user have permission?"
4. No ACLs exist → **ACCESS DENIED**

**Solution**: Use IAM authentication to bootstrap initial ACLs, then switch to SASL/SCRAM for applications.

### Authentication Ports Reference

| Port | Authentication | Use Case |
|------|---------------|----------|
| 9092 | Plaintext | Development only (NOT recommended) |
| 9094 | mTLS | High security with certificates |
| 9096 | SASL/SCRAM | Username/password for applications |
| 9098 | IAM | AWS-native apps, admin operations |
| 9196 | SASL/SCRAM (public) | External access |
| 9198 | IAM (public) | External access with IAM |

### Step 7.1: Create IAM Policy for MSK Access

```bash
cat > msk-admin-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "kafka-cluster:Connect",
        "kafka-cluster:AlterCluster",
        "kafka-cluster:DescribeCluster"
      ],
      "Resource": "arn:aws:kafka:ap-southeast-1:<aws-account-id>:cluster/demo-cluster/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "kafka-cluster:*Topic*",
        "kafka-cluster:WriteData",
        "kafka-cluster:ReadData"
      ],
      "Resource": "arn:aws:kafka:ap-southeast-1:<aws-account-id>:topic/demo-cluster/*/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "kafka-cluster:AlterGroup",
        "kafka-cluster:DescribeGroup"
      ],
      "Resource": "arn:aws:kafka:ap-southeast-1:<aws-account-id>:group/demo-cluster/*/*"
    }
  ]
}
EOF

# Create the IAM policy
aws iam create-policy \
  --policy-name MSK-Admin-Full-Access \
  --policy-document file://msk-admin-policy.json \
  --description "Full administrative access to MSK cluster"
```

### Step 7.2: Create IAM Role for EC2

```bash
# Create trust policy
cat > trust-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "ec2.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

# Create role
aws iam create-role \
  --role-name EC2-MSK-Admin-Role \
  --assume-role-policy-document file://trust-policy.json \
  --description "Role for EC2 instances to administer MSK cluster"

# Attach policy
aws iam attach-role-policy \
  --role-name EC2-MSK-Admin-Role \
  --policy-arn arn:aws:iam::<aws-account-id>:policy/MSK-Admin-Full-Access
```

### Step 7.3: Create Instance Profile and Attach to EC2

```bash
# Create instance profile
aws iam create-instance-profile \
  --instance-profile-name EC2-MSK-Admin-Profile

# Add role to profile
aws iam add-role-to-instance-profile \
  --instance-profile-name EC2-MSK-Admin-Profile \
  --role-name EC2-MSK-Admin-Role

# Wait for propagation
sleep 10

# Get instance ID
INSTANCE_ID="i-your-instance-id"

# Attach to EC2
aws ec2 associate-iam-instance-profile \
  --instance-id $INSTANCE_ID \
  --iam-instance-profile Name=EC2-MSK-Admin-Profile \
  --region ap-southeast-1

# Wait for IAM propagation
sleep 30
```

### Step 7.4: Configure Kafka Client for IAM (on EC2)

```bash
# Create IAM client properties
cat > ~/iam-client.properties << 'EOF'
security.protocol=SASL_SSL
sasl.mechanism=AWS_MSK_IAM
sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;
sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler
EOF

# Set environment variables
cat >> ~/.bashrc << 'EOF'

# MSK Configuration
export CLASSPATH=~/kafka_2.13-3.6.0/libs/aws-msk-iam-auth-1.1.6-all.jar
export IAM_BROKERS="<msk-broker>:9098,<msk-broker>:9098"
EOF

source ~/.bashrc
```

### Step 7.5: Create Kafka ACLs

```bash
# Add ACL for all topics
~/kafka_2.13-3.6.0/bin/kafka-acls.sh \
  --bootstrap-server $IAM_BROKERS \
  --command-config ~/iam-client.properties \
  --add \
  --allow-principal "User:pcleene" \
  --operation ALL \
  --topic '*' \
  --cluster

# Add ACL for all consumer groups
~/kafka_2.13-3.6.0/bin/kafka-acls.sh \
  --bootstrap-server $IAM_BROKERS \
  --command-config ~/iam-client.properties \
  --add \
  --allow-principal "User:pcleene" \
  --operation ALL \
  --group '*'

# Verify ACLs
~/kafka_2.13-3.6.0/bin/kafka-acls.sh \
  --bootstrap-server $IAM_BROKERS \
  --command-config ~/iam-client.properties \
  --list
```

---

## Part 8: RDS PostgreSQL CDC Configuration

### Step 8.1: Create Custom Parameter Group

**Via AWS Console:**
1. Go to **RDS → Parameter Groups**
2. Click **Create parameter group**
3. Configure:
   - Parameter group family: `postgres17` (or your version)
   - Type: `DB Parameter Group`
   - Group name: `paulDebeziumSync`
   - Description: `parameter group for debezium connect on msk`

**Via CLI:**
```bash
aws rds create-db-parameter-group \
    --db-parameter-group-name Utility-crm-logical-replication \
    --db-parameter-group-family postgres14 \
    --description "Enable logical replication for Debezium CDC" \
    --region ap-southeast-1
```

### Step 8.2: Enable Logical Replication

Edit the parameter group with these values:

| Parameter | Value | Description |
|-----------|-------|-------------|
| `rds.logical_replication` | 1 | **Critical** - enables CDC |
| `wal_level` | logical | May already be set |
| `max_replication_slots` | 5 | Number of replication slots |
| `max_wal_senders` | 5 | Number of WAL sender processes |

```bash
aws rds modify-db-parameter-group \
    --db-parameter-group-name Utility-crm-logical-replication \
    --parameters "ParameterName=rds.logical_replication,ParameterValue=1,ApplyMethod=pending-reboot" \
    --region ap-southeast-1
```

### Step 8.3: Attach Parameter Group to RDS

```bash
aws rds modify-db-instance \
    --db-instance-identifier Utilitycustomerdatasql \
    --db-parameter-group-name Utility-crm-logical-replication \
    --apply-immediately \
    --region ap-southeast-1
```

### Step 8.4: Reboot RDS Instance

> ⚠️ **Required**: Reboot is necessary for `rds.logical_replication` to take effect.

```bash
aws rds reboot-db-instance \
    --db-instance-identifier Utilitycustomerdatasql \
    --region ap-southeast-1
```

### Step 8.5: Create Debezium User in PostgreSQL

Connect to RDS:
```bash
psql -h <rds-endpoint> -U postgres -d Utility_crm
```

Create CDC user:
```sql
-- Create user and grant RDS replication role
CREATE USER debezium WITH PASSWORD '<password>';
GRANT rds_replication TO debezium;

-- Grant necessary permissions
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;

-- Grant CREATE permission (for publications)
GRANT CREATE ON DATABASE Utility_crm TO debezium;
GRANT CONNECT ON DATABASE Utility_crm TO debezium;

-- Grant superuser (for creating replication slots)
GRANT rds_superuser TO debezium;

-- Verify
SELECT rolname, rolreplication FROM pg_roles WHERE rolname = 'debezium';
-- Should show: debezium | t
```

### Step 8.6: Verify Logical Replication

```sql
SHOW wal_level;
-- Should return: logical

SHOW max_replication_slots;
-- Should return: 5

SELECT * FROM pg_replication_slots;
-- Should be empty initially
```

---

## Part 9: Debezium Connector Setup

### Step 9.1: Download Debezium Connector

SSH to EC2 instance:

```bash
# Create working directory
mkdir -p ~/debezium-setup
cd ~/debezium-setup

# Download Debezium PostgreSQL connector
wget https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/2.4.2.Final/debezium-connector-postgres-2.4.2.Final-plugin.tar.gz

# Extract
tar -xzf debezium-connector-postgres-2.4.2.Final-plugin.tar.gz

# Verify contents
ls -la debezium-connector-postgres/
```

### Step 9.2: Create ZIP for MSK Connect

MSK Connect requires ZIP format:

```bash
# Install zip utility
sudo yum install zip -y  # Amazon Linux
# OR
sudo apt install zip -y  # Ubuntu

# Create ZIP file
zip -r debezium-postgres-connector.zip debezium-connector-postgres/

# Verify
ls -lh debezium-postgres-connector.zip
```

### Step 9.3: Upload to S3

```bash
# Create S3 bucket (if needed)
BUCKET_NAME="demo-msk-plugins-bucket"
aws s3 mb s3://$BUCKET_NAME --region ap-southeast-1

# Upload ZIP
aws s3 cp debezium-postgres-connector.zip s3://$BUCKET_NAME/plugins/

# Verify
aws s3 ls s3://$BUCKET_NAME/plugins/
```

### Step 9.4: Create MSK Connect Custom Plugin

**Via AWS Console:**
1. Go to **Amazon MSK → MSK Connect → Custom plugins**
2. Click **Create custom plugin**
3. Configure:
   - S3 URI: `s3://demo-msk-plugins-bucket/plugins/debezium-postgres-connector.zip`
   - Custom plugin name: `debezium-postgres-connector`
   - Description: `Debezium PostgreSQL CDC connector for RDS to MSK`
4. Wait for status to become **Active** (2-3 minutes)

---

## Part 10: MSK Connect Configuration

### Step 10.1: Create IAM Role for MSK Connect

**Trust Policy:**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "kafkaconnect.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

**Permissions Policy:**
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "kafka-cluster:Connect",
                "kafka-cluster:AlterCluster",
                "kafka-cluster:DescribeCluster"
            ],
            "Resource": "arn:aws:kafka:ap-southeast-1:<aws-account-id>:cluster/demo-cluster/*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "kafka-cluster:CreateTopic",
                "kafka-cluster:DescribeTopic",
                "kafka-cluster:AlterTopic",
                "kafka-cluster:DeleteTopic",
                "kafka-cluster:WriteData",
                "kafka-cluster:ReadData"
            ],
            "Resource": [
                "arn:aws:kafka:ap-southeast-1:<aws-account-id>:topic/demo-cluster/*/__amazon_msk_connect*",
                "arn:aws:kafka:ap-southeast-1:<aws-account-id>:topic/demo-cluster/*/crm*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "kafka-cluster:AlterGroup",
                "kafka-cluster:DescribeGroup"
            ],
            "Resource": "arn:aws:kafka:ap-southeast-1:<aws-account-id>:group/demo-cluster/*/__amazon_msk_connect*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "kafka-cluster:WriteDataIdempotently"
            ],
            "Resource": "arn:aws:kafka:ap-southeast-1:<aws-account-id>:cluster/demo-cluster/*"
        }
    ]
}
```

Also attach:
- `AmazonS3ReadOnlyAccess`
- `CloudWatchLogsFullAccess`

Role name: `msk-connect-debezium-role`

### Step 10.2: Allow MSK Connect to Reach Brokers

```bash
aws ec2 authorize-security-group-ingress \
    --group-id <sg-id> \
    --protocol tcp \
    --port 9098 \
    --source-group <sg-id> \
    --region ap-southeast-1
```

### Step 10.3: Create Connector Configuration

```json
{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "publication.autocreate.mode": "all_tables",
  "transforms.unwrap.delete.handling.mode": "rewrite",
  "database.user": "debezium",
  "database.dbname": "Utility_crm",
  "slot.name": "debezium_slot",
  "tasks.max": "1",
  "transforms": "unwrap",
  "time.precision.mode": "connect",
  "database.server.name": "crm",
  "database.port": "5432",
  "plugin.name": "pgoutput",
  "key.converter.schemas.enable": "false",
  "topic.prefix": "crm",
  "decimal.handling.mode": "double",
  "schema.history.internal.kafka.topic": "crm.schema-history",
  "database.hostname": "<rds-endpoint>",
  "transforms.unwrap.drop.tombstones": "false",
  "database.password": "<password>",
  "value.converter.schemas.enable": "true",
  "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "key.converter": "org.apache.kafka.connect.json.JsonConverter",
  "snapshot.mode": "initial"
}
```

### Configuration Parameters Explained

| Parameter | Value | Description |
|-----------|-------|-------------|
| `connector.class` | PostgresConnector | Debezium PostgreSQL connector |
| `plugin.name` | pgoutput | PostgreSQL logical decoding plugin |
| `slot.name` | debezium_slot | Replication slot name |
| `topic.prefix` | crm | Prefix for Kafka topics |
| `snapshot.mode` | initial | Take initial snapshot, then stream |
| `transforms` | unwrap | Use ExtractNewRecordState |

### Step 10.4: Create MSK Connector

**Via AWS Console:**
1. Go to **Amazon MSK → MSK Connect → Connectors**
2. Click **Create connector**
3. Select your custom plugin
4. Configure connector settings (paste JSON above)
5. Select your MSK cluster
6. Select IAM role: `msk-connect-debezium-role`
7. Configure worker settings (Kafka Connect version: 3.7.x)
8. Create connector

---

## Verification & Testing

### Verify PostgreSQL Publication

```sql
-- Check publication was created
SELECT * FROM pg_publication WHERE pubname = 'dbz_publication';

-- Check which tables are in the publication
SELECT schemaname, tablename
FROM pg_publication_tables
WHERE pubname = 'dbz_publication';

-- Check replication slot is active
SELECT slot_name, plugin, active, restart_lsn
FROM pg_replication_slots
WHERE slot_name = 'debezium_slot';
```

**Expected Output:**
```
 schemaname |     tablename
------------+--------------------
 public     | customers
 public     | customer_addresses
 public     | bills
 public     | bill_line_items
 public     | payments
 public     | support_tickets
 public     | ticket_comments
 public     | service_requests
(8 rows)

   slot_name   |  plugin  | active | restart_lsn
---------------+----------+--------+-------------
 debezium_slot | pgoutput | t      | 4/40000D40
(1 row)
```

### Verify Kafka Topics

```bash
~/kafka_2.13-3.6.0/bin/kafka-topics.sh \
  --bootstrap-server $IAM_BROKERS \
  --command-config ~/iam-client.properties \
  --list
```

### Check CloudWatch Logs

Look for these messages in connector logs:

1. **Publication created:**
   ```
   Created publication 'dbz_publication'
   ```

2. **Replication slot created:**
   ```
   Creating replication slot 'debezium_slot'
   ```

3. **Snapshot starting:**
   ```
   Starting snapshot for...
   Snapshot is using user 'debezium'
   ```

4. **Tables being captured:**
   ```
   Snapshotting table public.customers
   Snapshotting table public.bills
   ```

5. **Streaming started:**
   ```
   Snapshot completed
   Starting streaming
   ```

---

## Kafka Concepts Deep Dive

### Bootstrap Servers

**Definition:** Bootstrap servers are the initial Kafka brokers your client connects to for cluster discovery.

```
bootstrap.servers: "broker1:9092,broker2:9092,broker3:9092"
```

**Key Points:**
- Entry points for cluster discovery
- List multiple for redundancy
- Every Kafka broker can serve as a bootstrap server

### Consumer Groups

A **consumer group** is a logical grouping of consumers that coordinate to read from a topic.

**How it works:**
```
Topic: orders (3 partitions: P0, P1, P2)

Consumer Group: order-processor-group
  ├── Consumer C1 → P0
  ├── Consumer C2 → P1
  └── Consumer C3 → P2

Consumer Group: analytics-group (reads same topic independently)
  └── Consumer A1 → P0, P1, P2
```

**Key Points:**
- Each partition is read by only one consumer in the group
- Different groups consume the same topic independently
- Consumers track progress via committed offsets

### ACL Structure

```
Principal: User:pcleene
Operation: READ, WRITE, CREATE, DELETE, ALTER, DESCRIBE
Resource: Topic:my-topic, Group:my-group, Cluster:kafka-cluster
Permission: ALLOW or DENY
```

---

## Troubleshooting

### Network Issues

**Symptom:** Connection timeout

```bash
# Test basic connectivity
nc -zv broker-host 9098

# Check security groups
aws ec2 describe-security-groups --group-ids $MSK_SG
```

**Fixes:**
- Add security group rule for port 9098
- Verify EC2 and MSK are in same VPC
- Check VPC routing tables

### IAM Authentication Issues

**Symptom:** `SaslAuthenticationException: Access denied`

```bash
# Verify IAM role is attached
curl http://169.254.169.254/latest/meta-data/iam/security-credentials/

# Check IAM permissions
aws iam get-policy-version \
  --policy-arn YOUR_POLICY_ARN \
  --version-id v1
```

**Fixes:**
- Verify IAM policy has correct resource ARNs
- Wait for IAM propagation (30-60 seconds)

### ACL Issues

**Symptom:** `ClusterAuthorizationException`

This means you authenticated but don't have Kafka ACL permissions.

**Solution:** Use IAM authentication to create initial ACLs.

### Debezium Issues

**Symptom:** Connector fails to start

**Check:**
1. RDS security group allows inbound from MSK Connect
2. Debezium user has required permissions
3. `rds.logical_replication` is enabled
4. RDS was rebooted after parameter change

---

## Best Practices

### Security

1. **Use IAM for Admin, SASL for Applications**
   - IAM: Admin EC2 instances, AWS-native applications
   - SASL/SCRAM: External applications, microservices

2. **Principle of Least Privilege**
   - Give each application only needed permissions
   - Use specific topic patterns, not wildcards
   - Separate read and write permissions

3. **Rotate Credentials Regularly**
   - SASL/SCRAM passwords should be rotated
   - IAM temporary credentials rotate automatically

### Operational

1. **Use Configuration Management**
   - Store ACL configurations in version control
   - Use Infrastructure as Code (Terraform, CloudFormation)

2. **Monitor Access**
   - Enable MSK audit logging
   - Monitor CloudTrail for IAM access
   - Set up alerts for unauthorized access attempts

3. **Document Your ACLs**
   - Keep a spreadsheet of user → permissions mapping
   - Document why each permission was granted

---

## Quick Reference

### Get MSK Information

```bash
# List clusters
aws kafka list-clusters --region ap-southeast-1

# Get bootstrap brokers
aws kafka get-bootstrap-brokers --cluster-arn $CLUSTER_ARN

# Describe cluster
aws kafka describe-cluster --cluster-arn $CLUSTER_ARN
```

### Common Kafka Commands

```bash
# List topics
~/kafka_2.13-3.6.0/bin/kafka-topics.sh \
  --bootstrap-server $IAM_BROKERS \
  --command-config ~/iam-client.properties \
  --list

# Create topic
~/kafka_2.13-3.6.0/bin/kafka-topics.sh \
  --bootstrap-server $IAM_BROKERS \
  --command-config ~/iam-client.properties \
  --create \
  --topic my-topic \
  --partitions 3 \
  --replication-factor 2

# Produce messages
~/kafka_2.13-3.6.0/bin/kafka-console-producer.sh \
  --bootstrap-server $IAM_BROKERS \
  --producer.config ~/iam-client.properties \
  --topic my-topic

# Consume messages
~/kafka_2.13-3.6.0/bin/kafka-console-consumer.sh \
  --bootstrap-server $IAM_BROKERS \
  --consumer.config ~/iam-client.properties \
  --topic my-topic \
  --from-beginning
```

### Environment Variables Template

```bash
# Add to ~/.bashrc
export AWS_REGION="ap-southeast-1"
export CLUSTER_ARN="arn:aws:kafka:ap-southeast-1:<aws-account-id>:cluster/demo-cluster/CLUSTER_UUID"
export IAM_BROKERS="<msk-broker>:9098,<msk-broker>:9098"
export CLASSPATH=~/kafka_2.13-3.6.0/libs/aws-msk-iam-auth-1.1.6-all.jar
```

---

## Summary Checklist

- [ ] Atlas Stream Processing IPs allowlisted
- [ ] AWS Secrets Manager secret created with KMS encryption
- [ ] Security groups configured for MSK, EC2, and RDS
- [ ] MSK cluster configuration created
- [ ] MSK cluster deployed and public access enabled
- [ ] EC2 admin instance launched with IAM role
- [ ] Kafka tools and IAM auth library installed
- [ ] Kafka ACLs created for admin user
- [ ] RDS parameter group with logical replication enabled
- [ ] RDS rebooted after parameter change
- [ ] Debezium user created in PostgreSQL
- [ ] Debezium connector plugin uploaded to S3
- [ ] MSK Connect custom plugin created
- [ ] MSK Connect IAM role created
- [ ] MSK Connect connector deployed
- [ ] CDC verified (publication, replication slot, topics)

---

*Guide Version: 1.0*
*Last Updated: January 2026*
*Author: Paul Cleenewerck*
