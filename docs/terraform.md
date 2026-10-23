# Terraform Infrastructure Guide (Issue #650)

SorobanPulse's cloud infrastructure is defined as code using [Terraform](https://www.terraform.io/) (>= 1.6). All resources — VPC, RDS PostgreSQL, Application Load Balancer, and monitoring — live under `terraform/`.

## Directory Structure

```
terraform/
├── providers.tf                    # AWS provider + S3 backend config
├── main.tf                         # Root module — wires sub-modules together
├── variables.tf                    # All input variables with descriptions
├── outputs.tf                      # Root-level outputs
├── terraform.tfvars.example        # Template for your tfvars file
│
├── modules/
│   ├── vpc/                        # VPC, subnets, security groups, NAT
│   │   ├── main.tf
│   │   ├── variables.tf
│   │   └── outputs.tf
│   ├── rds/                        # RDS PostgreSQL, parameter group, Secrets Manager
│   │   ├── main.tf
│   │   ├── variables.tf
│   │   └── outputs.tf
│   ├── alb/                        # Application Load Balancer, listeners, target group
│   │   ├── main.tf
│   │   ├── variables.tf
│   │   └── outputs.tf
│   └── monitoring/                 # CloudWatch alarms, SNS topic, log group, dashboard
│       ├── main.tf
│       ├── variables.tf
│       └── outputs.tf
│
└── environments/
    ├── staging/
    │   └── terraform.tfvars.example
    └── production/
        └── terraform.tfvars.example
```

## Prerequisites

| Tool | Minimum Version |
|------|----------------|
| [Terraform](https://developer.hashicorp.com/terraform/downloads) | 1.6 |
| [AWS CLI](https://aws.amazon.com/cli/) | 2.x |
| AWS credentials | Admin (or scoped IAM role) |

## Bootstrapping Remote State

Terraform state is stored in S3 with DynamoDB locking. Create these once **before** your first `terraform init`:

```bash
# Create the state bucket
aws s3api create-bucket \
  --bucket soroban-pulse-terraform-state \
  --region us-east-1

# Enable versioning (protect against accidental state deletion)
aws s3api put-bucket-versioning \
  --bucket soroban-pulse-terraform-state \
  --versioning-configuration Status=Enabled

# Enable encryption at rest
aws s3api put-bucket-encryption \
  --bucket soroban-pulse-terraform-state \
  --server-side-encryption-configuration \
    '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'

# Create the DynamoDB lock table
aws dynamodb create-table \
  --table-name soroban-pulse-terraform-locks \
  --attribute-definitions AttributeName=LockID,AttributeType=S \
  --key-schema AttributeName=LockID,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --region us-east-1
```

## First-Time Setup

```bash
cd terraform

# 1. Copy and fill in variables
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars — at minimum set:
#   environment, certificate_arn

# 2. Initialise providers and backend
terraform init

# 3. Preview changes
terraform plan

# 4. Apply
terraform apply
```

## Managing Environments

Each environment (staging, production) can use the same Terraform root module with different variable files:

```bash
# Staging
cd terraform
terraform workspace new staging    # or: terraform workspace select staging
cp environments/staging/terraform.tfvars.example terraform.tfvars
# fill in terraform.tfvars
terraform apply

# Production
terraform workspace new production
cp environments/production/terraform.tfvars.example terraform.tfvars
# fill in terraform.tfvars
terraform apply
```

Alternatively, use `-var-file`:

```bash
terraform apply -var-file="environments/production/terraform.tfvars"
```

## Module Reference

### `modules/vpc`

Creates the core networking layer.

| Resource | Description |
|----------|-------------|
| `aws_vpc` | VPC with DNS hostnames enabled |
| `aws_subnet` (public) | One per AZ, used by the ALB |
| `aws_subnet` (private) | One per AZ, used by RDS and the app |
| `aws_internet_gateway` | Internet egress for public subnets |
| `aws_nat_gateway` | Egress for private subnets (1 or per-AZ) |
| `aws_security_group` (alb) | Allows 80/443 inbound from internet |
| `aws_security_group` (app) | Allows app port inbound from ALB only |
| `aws_security_group` (rds) | Allows 5432 inbound from app SG only |

**Key outputs:** `vpc_id`, `public_subnet_ids`, `private_subnet_ids`, `alb_security_group_id`, `app_security_group_id`, `rds_security_group_id`

---

### `modules/rds`

Creates a managed PostgreSQL 16 instance.

| Resource | Description |
|----------|-------------|
| `aws_db_instance` | PostgreSQL 16, gp3, encrypted at rest |
| `aws_db_subnet_group` | Spans all private subnets |
| `aws_db_parameter_group` | SSL enforcement, slow query logging, autovacuum tuning |
| `aws_secretsmanager_secret` | Stores auto-generated DB password |
| `random_password` | 32-char random password (never in state plaintext) |
| `aws_iam_role` | Enhanced Monitoring role (when interval > 0) |

**Key outputs:** `db_instance_id`, `db_endpoint` (sensitive), `db_secret_arn` (sensitive)

**Connecting the application:**

The application reads `DATABASE_URL` from an environment variable. In ECS, inject it by fetching the Secrets Manager secret:

```json
{
  "name": "DATABASE_URL",
  "valueFrom": "arn:aws:secretsmanager:us-east-1:ACCOUNT:secret:soroban-pulse-staging/rds/credentials"
}
```

---

### Sizing variables for multi-replica deployments

These root-module variables interact with the application's connection pool.
Derive them from the formulas in
[Capacity Planning → Multi-replica sizing](capacity-planning.md#multi-replica-sizing).

| Variable | Default | Formula / rule |
|----------|---------|----------------|
| `app_container_count` (`N`) | `2` | Primary connections `C_primary = N × DB_MAX_CONNECTIONS`. During a rolling deploy `C_peak = ceil(2 × N) × DB_MAX_CONNECTIONS` (the ECS module sets `deployment_maximum_percent = 200`). |
| `db_instance_class` | `db.t3.medium` | Sets PostgreSQL `max_connections` `M = LEAST(DBInstanceClassMemory / 9531392, 5000)` (≈ 410 for `db.t3.medium`). Require `C_peak ≤ 0.8 × (M − 10)`. |
| `ecs_task_cpu` | `512` | Every task can become the indexer leader, so size for leader CPU, not the standby baseline. |
| `ecs_task_memory` | `1024` | Same as a single instance: pool memory grows only slightly with `DB_MAX_CONNECTIONS`. |

`DB_MAX_CONNECTIONS` is an application setting (default `10`). The
Terraform module does not set it. To override it, add it to
`environment_variables` in `module "ecs"` (`terraform/main.tf`).

With the defaults (`N = 2`, `DB_MAX_CONNECTIONS = 10`, `db.t3.medium`),
`C_peak = 40`, well within the budget of `0.8 × (410 − 10) = 320`.

---

### `modules/alb`

Creates an HTTPS-terminating Application Load Balancer.

| Resource | Description |
|----------|-------------|
| `aws_lb` | Application Load Balancer |
| `aws_lb_target_group` | IP-based target group (ECS Fargate compatible) |
| `aws_lb_listener` (HTTP) | Redirects port 80 → 443 |
| `aws_lb_listener` (HTTPS) | Forwards to target group with TLS 1.3 policy |
| `aws_s3_bucket` | Optional access log bucket (90-day retention) |

The health check target is `GET /healthz/ready`. The ALB uses TLS policy `ELBSecurityPolicy-TLS13-1-2-2021-06` (supports TLS 1.2 and 1.3 only).

**Key outputs:** `alb_dns_name`, `alb_zone_id`, `target_group_arn`

---

### `modules/monitoring`

Creates CloudWatch alarms, an SNS topic, log group, and a dashboard.

| Alarm | Metric | Default Threshold |
|-------|--------|-------------------|
| `rds-cpu-high` | RDS CPUUtilization | 80% |
| `rds-storage-low` | RDS FreeStorageSpace | 5 GiB |
| `rds-connections-high` | RDS DatabaseConnections | 80 |
| `rds-read-latency` | RDS ReadLatency (p99) | 20ms |
| `alb-5xx-high` | ALB HTTPCode_Target_5XX_Count | 10/min |
| `alb-p99-latency` | ALB TargetResponseTime (p99) | 1.0s |
| `alb-unhealthy-hosts` | ALB UnHealthyHostCount | ≥ 1 |

All alarms publish to an SNS topic (`<name-prefix>-alarms`). Pass `alarm_actions_arn` to forward alerts to PagerDuty, OpsGenie, or Slack.

**Key outputs:** `log_group_name`, `sns_alarm_topic_arn`, `dashboard_url`

## Obtaining an ACM Certificate

The HTTPS listener requires an ACM certificate ARN. Request one via:

```bash
aws acm request-certificate \
  --domain-name api.yourdomain.com \
  --validation-method DNS \
  --region us-east-1
```

Then add the CNAME validation record to your DNS and wait for status to become `ISSUED`.

## Security Notes

- **DB password** is auto-generated by `random_password` and stored only in Secrets Manager — it is never visible in plaintext Terraform state (which is stored encrypted in S3).
- **RDS** is `publicly_accessible = false` and only reachable from the app security group.
- **SSL** is enforced via the `rds.force_ssl = 1` parameter group setting.
- **ALB** enforces TLS 1.2+ and redirects all HTTP traffic to HTTPS.
- State bucket has versioning and SSE enabled.

## CI/CD Integration

Terraform changes are automated through two GitHub Actions workflows. No manual `terraform apply` is needed for the normal flow.

> **TL;DR** — PRs touching `terraform/` show the plan as a comment; merging to `main` applies to **staging** automatically.

### `terraform-ci.yml` — Plan on Pull Request

Triggered on any PR that modifies files under `terraform/`. It:

1. Runs `terraform fmt -check -recursive` (fails the job on unformatted files).
2. Runs `terraform init` against the S3 backend and `terraform validate`.
3. Runs `terraform plan` for the target workspace.
4. Posts the plan output as a **PR comment** (updated in place on each push) using [`hashicorp/setup-terraform`](https://github.com/hashicorp/setup-terraform).

The plan is generated with `-lock=false` and never applied, so PRs are read-only against infrastructure.

### `terraform-apply.yml` — Apply on Merge to `main`

Triggered on push to `main` when `terraform/` changes. It runs `terraform apply` for the **staging** environment only. Production is never applied automatically — it must be run manually (see below).

### Required Repository Secrets

| Secret | Purpose |
|--------|---------|
| `TF_AWS_ACCESS_KEY_ID` | IAM credentials for Terraform |
| `TF_AWS_SECRET_ACCESS_KEY` | IAM credentials for Terraform |
| `ACM_CERT_ARN` | Value for `TF_VAR_certificate_arn` |

### Selecting an Environment with `TF_WORKSPACE`

Both workflows accept a `TF_WORKSPACE` input (workflow_dispatch) that selects the Terraform workspace, which maps 1:1 to an environment:

| `TF_WORKSPACE` | Environment | Applied by CI |
|----------------|-------------|---------------|
| `dev` | Development | Manual only |
| `staging` | Staging | Automatically on merge to `main` |
| `production` | Production | Manual only (requires approval) |

`TF_WORKSPACE` is exported to the Terraform CLI, so `terraform init`/`plan`/`apply` operate on the matching workspace and state prefix. To run a workflow for a specific environment, use **Actions → (workflow) → Run workflow** and set `TF_WORKSPACE`.

To apply production manually:

```bash
cd terraform
terraform init
terraform workspace select production
terraform apply -var-file="environments/production/terraform.tfvars"
```

### Remote State Backend (S3 + DynamoDB)

State lives in S3 with DynamoDB locking (configured in `providers.tf`). The bucket and lock table are created once by the bootstrap script:

```bash
./scripts/terraform-init.sh
```

This creates the versioned, encrypted `soroban-pulse-terraform-state` bucket and the `soroban-pulse-terraform-locks` DynamoDB table (see [Bootstrapping Remote State](#bootstrapping-remote-state) for the equivalent manual commands). CI authenticates to the same backend, so local and CI runs share state and locking.

## Common Commands

```bash
# Preview changes
terraform plan

# Apply changes
terraform apply

# Destroy everything (dangerous — requires confirmation)
terraform destroy

# Show current state
terraform show

# List resources
terraform state list

# Import an existing resource
terraform import module.rds.aws_db_instance.main my-existing-db

# Refresh state against real infrastructure
terraform refresh

# Format all .tf files
terraform fmt -recursive

# Validate configuration
terraform validate
```

## Troubleshooting

**`Error: Backend initialization required`**
Run `terraform init` first. If the S3 bucket doesn't exist yet, follow the bootstrap steps above.

**`Error: certificate_arn is required`**
Set `certificate_arn` in your `terraform.tfvars` or via `TF_VAR_certificate_arn`.

**`Error acquiring the state lock`**
Another process is running. If it's stale: `terraform force-unlock <LOCK_ID>`.

**RDS deletion fails with `deletion_protection`**
Set `db_deletion_protection = false`, apply, then destroy. Always use `true` in production.
