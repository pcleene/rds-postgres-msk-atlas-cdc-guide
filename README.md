<!-- Portfolio repository -->

> **RDS → MSK → Atlas Stream Processing CDC Guide** — portfolio demonstration.
> Debezium CDC reference architecture for PostgreSQL to MongoDB via Kafka
>
> This is a sanitized public version of a real-world prototype. Client names,
> credentials, internal endpoints, and proprietary assets have been removed; all
> configuration is environment-driven (`.env.example`). Authored by
> [Paul Cleenewerck](https://github.com/pcleene).

---

# RDS to MSK CDC with Debezium - Complete Guide

A comprehensive guide for setting up Change Data Capture (CDC) from Amazon RDS PostgreSQL to Amazon MSK using Debezium, with MongoDB Atlas Stream Processing integration.

## What's Included

- **RDS-to-MSK-Debezium-CDC-Complete-Guide.md** - The complete step-by-step guide covering:
  - Architecture overview and diagrams
  - Prerequisites and configuration values
  - Atlas Stream Processing IP configuration
  - AWS Secrets Manager setup
  - Security groups configuration
  - MSK cluster setup and deployment
  - EC2 admin instance configuration
  - Kafka authentication and ACLs (IAM & SASL/SCRAM)
  - RDS PostgreSQL logical replication setup
  - Debezium connector deployment
  - MSK Connect configuration
  - Verification and testing procedures
  - Troubleshooting guide
  - Best practices

## Quick Start

1. Review the prerequisites section in the guide
2. Follow the step-by-step instructions
3. Use the verification section to confirm your setup

## Technologies Covered

- **Amazon RDS PostgreSQL** - Source database with CDC
- **Amazon MSK** - Managed Kafka service
- **Debezium** - CDC connector
- **MSK Connect** - Managed Kafka Connect
- **MongoDB Atlas Stream Processing** - Data processing destination
- **AWS IAM** - Authentication and authorization
- **AWS Secrets Manager** - Credential management

## Author

Paul Cleenewerck - MongoDB

## License

Private - Internal use only
