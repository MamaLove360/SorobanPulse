# =============================================================================
# Input Variables — SorobanPulse Terraform (Issue #650, #833)
# =============================================================================

# ---------------------------------------------------------------------------
# Global
# ---------------------------------------------------------------------------

variable "aws_region" {
  description = "AWS region in which all resources are created."
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment: dev | staging | production."
  type        = string
  validation {
    condition     = contains(["dev", "staging", "production"], var.environment)
    error_message = "environment must be 'dev', 'staging', or 'production'."
  }
}

variable "project_name" {
  description = "Short name used as a prefix on all resource names."
  type        = string
  default     = "soroban-pulse"
}

# ---------------------------------------------------------------------------
# Networking / VPC
# ---------------------------------------------------------------------------

variable "vpc_cidr" {
  description = "CIDR block for the VPC."
  type        = string
  default     = "10.0.0.0/16"
}

variable "availability_zones" {
  description = "List of availability zones to spread resources across (min 2)."
  type        = list(string)
  default     = ["us-east-1a", "us-east-1b", "us-east-1c"]
}

variable "public_subnet_cidrs" {
  description = "CIDR blocks for public subnets (one per AZ — used by the ALB)."
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
}

variable "private_subnet_cidrs" {
  description = "CIDR blocks for private subnets (one per AZ — used by RDS and the app)."
  type        = list(string)
  default     = ["10.0.11.0/24", "10.0.12.0/24", "10.0.13.0/24"]
}

variable "enable_nat_gateway" {
  description = "Whether to provision NAT Gateway(s) for private subnet egress."
  type        = bool
  default     = true
}

variable "single_nat_gateway" {
  description = "Use a single NAT Gateway (cheaper) instead of one per AZ."
  type        = bool
  default     = false
}

# ---------------------------------------------------------------------------
# RDS (PostgreSQL)
# ---------------------------------------------------------------------------

variable "db_instance_class" {
  description = "RDS instance class. Its memory sets PostgreSQL max_connections (M); keep ceil(2 × app_container_count) × DB_MAX_CONNECTIONS ≤ 0.8 × (M − 10). See docs/capacity-planning.md#multi-replica-sizing."
  type        = string
  default     = "db.t3.medium"
}

variable "db_engine_version" {
  description = "PostgreSQL engine version."
  type        = string
  default     = "16.3"
}

variable "db_name" {
  description = "Name of the PostgreSQL database to create."
  type        = string
  default     = "soroban_pulse"
}

variable "db_username" {
  description = "Master username for the RDS instance."
  type        = string
  default     = "soroban_admin"
  sensitive   = true
}

variable "db_allocated_storage" {
  description = "Initial storage allocation in GiB."
  type        = number
  default     = 20
}

variable "db_max_allocated_storage" {
  description = "Maximum autoscaling storage in GiB (0 = disabled)."
  type        = number
  default     = 100
}

variable "db_backup_retention_period" {
  description = "Number of days to retain automated RDS backups (0 disables backups)."
  type        = number
  default     = 7
}

variable "db_multi_az" {
  description = "Enable Multi-AZ deployment for high availability."
  type        = bool
  default     = true
}

variable "db_deletion_protection" {
  description = "Prevent the RDS instance from being accidentally deleted."
  type        = bool
  default     = true
}

variable "db_apply_immediately" {
  description = "Apply changes immediately instead of during the next maintenance window."
  type        = bool
  default     = false
}

variable "db_skip_final_snapshot" {
  description = "Skip creating a final snapshot on instance deletion. Set to false in production."
  type        = bool
  default     = false
}

variable "db_performance_insights_enabled" {
  description = "Enable Performance Insights on the RDS instance."
  type        = bool
  default     = true
}

variable "db_monitoring_interval" {
  description = "Enhanced Monitoring interval in seconds (0 disables it)."
  type        = number
  default     = 60
}

# ---------------------------------------------------------------------------
# Application Load Balancer
# ---------------------------------------------------------------------------

variable "alb_internal" {
  description = "Create an internal (VPC-only) load balancer instead of internet-facing."
  type        = bool
  default     = false
}

variable "alb_access_logs_enabled" {
  description = "Enable access logging for the ALB to S3."
  type        = bool
  default     = true
}

variable "alb_idle_timeout" {
  description = "ALB idle connection timeout in seconds."
  type        = number
  default     = 60
}

variable "health_check_path" {
  description = "HTTP path used by the ALB target group health check."
  type        = string
  default     = "/healthz/ready"
}

variable "health_check_interval" {
  description = "Seconds between ALB health checks."
  type        = number
  default     = 30
}

variable "health_check_threshold" {
  description = "Number of consecutive successes required to mark a target healthy."
  type        = number
  default     = 2
}

variable "certificate_arn" {
  description = "ARN of the ACM certificate to attach to the HTTPS listener (required)."
  type        = string
}

# ---------------------------------------------------------------------------
# Application (ECS / target group)
# ---------------------------------------------------------------------------

variable "app_port" {
  description = "Port the SorobanPulse container listens on."
  type        = number
  default     = 3000
}

variable "app_container_count" {
  description = "Desired number of running application containers (N). Each opens its own DB_MAX_CONNECTIONS pool; size with C_peak = ceil(2 × N) × DB_MAX_CONNECTIONS. See docs/capacity-planning.md#multi-replica-sizing."
  type        = number
  default     = 2
}

# ---------------------------------------------------------------------------
# ECS Fargate (Issue #833)
# ---------------------------------------------------------------------------

variable "ecs_task_cpu" {
  description = "CPU units for the Fargate task (256 = 0.25 vCPU). Every task must fit the indexer leader workload. See docs/capacity-planning.md#standby-indexer-cpu-baseline."
  type        = number
  default     = 512
}

variable "ecs_task_memory" {
  description = "Memory in MiB for the Fargate task."
  type        = number
  default     = 1024
}

variable "ecs_container_image" {
  description = "Docker image URI for the application container."
  type        = string
  default     = "ghcr.io/soroban-pulse/sorobanpulse:latest"
}

# ---------------------------------------------------------------------------
# Backup (Issue #833)
# ---------------------------------------------------------------------------

variable "backup_retention_days" {
  description = "Number of days to retain S3 backups before expiration."
  type        = number
  default     = 90
}

variable "backup_force_destroy" {
  description = "Allow the backup bucket to be destroyed even with objects inside."
  type        = bool
  default     = false
}

# ---------------------------------------------------------------------------
# Monitoring & Alerting
# ---------------------------------------------------------------------------

variable "alarm_actions_arn" {
  description = "ARN(s) of SNS topics to notify when alarms fire (e.g. PagerDuty endpoint)."
  type        = list(string)
  default     = []
}

variable "ok_actions_arn" {
  description = "ARN(s) of SNS topics to notify when alarms recover."
  type        = list(string)
  default     = []
}

variable "rds_cpu_alarm_threshold" {
  description = "CPU utilisation % that triggers an RDS alarm."
  type        = number
  default     = 80
}

variable "rds_storage_alarm_threshold" {
  description = "Free storage bytes below which an RDS alarm fires."
  type        = number
  default     = 5368709120 # 5 GiB
}

variable "alb_5xx_alarm_threshold" {
  description = "Number of 5xx responses per minute that triggers an ALB alarm."
  type        = number
  default     = 10
}

variable "alb_latency_alarm_threshold" {
  description = "p99 response time in seconds that triggers an ALB latency alarm."
  type        = number
  default     = 1.0
}

variable "log_retention_days" {
  description = "CloudWatch log retention period in days."
  type        = number
  default     = 30
}
