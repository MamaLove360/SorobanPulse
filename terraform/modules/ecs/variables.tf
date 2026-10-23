# =============================================================================
# Module: ecs — Variables (Issue #833)
# =============================================================================

variable "name_prefix" {
  description = "Prefix applied to every resource name."
  type        = string
}

variable "aws_region" {
  description = "AWS region for CloudWatch log configuration."
  type        = string
}

variable "container_image" {
  description = "Docker image URI for the application container."
  type        = string
  default     = "ghcr.io/soroban-pulse/sorobanpulse:latest"
}

variable "task_cpu" {
  description = "CPU units for the Fargate task (256 = 0.25 vCPU)."
  type        = number
  default     = 512
}

variable "task_memory" {
  description = "Memory in MiB for the Fargate task."
  type        = number
  default     = 1024
}

variable "desired_count" {
  description = "Desired number of running task instances. DB connections scale as desired_count × DB_MAX_CONNECTIONS (×2 during deploys). See docs/capacity-planning.md#multi-replica-sizing."
  type        = number
  default     = 2
}

variable "app_port" {
  description = "Port the application container listens on."
  type        = number
  default     = 3000
}

variable "health_check_path" {
  description = "HTTP path for container health checks."
  type        = string
  default     = "/healthz/ready"
}

variable "private_subnet_ids" {
  description = "IDs of private subnets where tasks are launched."
  type        = list(string)
}

variable "security_group_ids" {
  description = "Security group IDs to attach to the ECS tasks."
  type        = list(string)
}

variable "target_group_arn" {
  description = "ARN of the ALB target group to register tasks with."
  type        = string
}

variable "secret_arns" {
  description = "ARNs of Secrets Manager secrets the execution role may read."
  type        = list(string)
  default     = []
}

variable "environment_variables" {
  description = "Environment variables passed to the container."
  type = list(object({
    name  = string
    value = string
  }))
  default = []
}

variable "secret_environment_variables" {
  description = "Secret environment variables (references to Secrets Manager or SSM)."
  type = list(object({
    name      = string
    valueFrom = string
  }))
  default = []
}

variable "container_insights" {
  description = "Enable CloudWatch Container Insights on the ECS cluster."
  type        = bool
  default     = true
}

variable "log_retention_days" {
  description = "CloudWatch log retention period in days for ECS task logs."
  type        = number
  default     = 30
}
