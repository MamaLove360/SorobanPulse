# =============================================================================
# Root Module — SorobanPulse Infrastructure (Issue #650, #833, #1283)
#
# Wires together:
#   - VPC + subnets + NAT gateway
#   - RDS PostgreSQL
#   - Application Load Balancer
#   - ECS Fargate service
#   - Automated backup storage
#   - Monitoring / CloudWatch alarms
#
# Remote state is stored in S3 with DynamoDB locking (see docs/terraform.md).
# The `environment` variable drives the name_prefix for every module, so the
# same configuration can target dev/staging/production via TF_WORKSPACE.
# =============================================================================

terraform {
  backend "s3" {
    bucket         = "soroban-pulse-terraform-state"
    key            = "terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "soroban-pulse-terraform-locks"
    encrypt        = true
  }
}

locals {
  name_prefix = "${var.project_name}-${var.environment}"
}

# ---------------------------------------------------------------------------
# Networking
# ---------------------------------------------------------------------------

module "vpc" {
  source = "./modules/vpc"

  name_prefix          = local.name_prefix
  vpc_cidr             = var.vpc_cidr
  availability_zones   = var.availability_zones
  public_subnet_cidrs  = var.public_subnet_cidrs
  private_subnet_cidrs = var.private_subnet_cidrs
  enable_nat_gateway   = var.enable_nat_gateway
  single_nat_gateway   = var.single_nat_gateway

  tags = {
    Environment = var.environment
  }
}

# ---------------------------------------------------------------------------
# RDS PostgreSQL
# ---------------------------------------------------------------------------

module "rds" {
  source = "./modules/rds"

  name_prefix                     = local.name_prefix
  vpc_id                          = module.vpc.vpc_id
  private_subnet_ids              = module.vpc.private_subnet_ids
  allowed_security_group_ids      = [module.vpc.app_security_group_id]
  rds_security_group_id           = module.vpc.rds_security_group_id
  db_name                         = var.db_name
  db_username                     = var.db_username
  db_instance_class               = var.db_instance_class
  db_engine_version               = var.db_engine_version
  db_allocated_storage            = var.db_allocated_storage
  db_max_allocated_storage        = var.db_max_allocated_storage
  db_backup_retention_period      = var.db_backup_retention_period
  db_multi_az                     = var.db_multi_az
  db_deletion_protection          = var.db_deletion_protection
  db_apply_immediately            = var.db_apply_immediately
  db_skip_final_snapshot          = var.db_skip_final_snapshot
  db_performance_insights_enabled = var.db_performance_insights_enabled
  db_monitoring_interval          = var.db_monitoring_interval
}

# ---------------------------------------------------------------------------
# Application Load Balancer
# ---------------------------------------------------------------------------

module "alb" {
  source = "./modules/alb"

  name_prefix            = local.name_prefix
  vpc_id                 = module.vpc.vpc_id
  public_subnet_ids      = module.vpc.public_subnet_ids
  alb_security_group_id  = module.vpc.alb_security_group_id
  internal               = var.alb_internal
  access_logs_enabled    = var.alb_access_logs_enabled
  idle_timeout           = var.alb_idle_timeout
  certificate_arn        = var.certificate_arn
  health_check_path      = var.health_check_path
  health_check_interval  = var.health_check_interval
  health_check_threshold = var.health_check_threshold
  app_port               = var.app_port
}

# ---------------------------------------------------------------------------
# ECS Fargate Service (Issue #833)
# ---------------------------------------------------------------------------

module "ecs" {
  source = "./modules/ecs"

  name_prefix        = local.name_prefix
  aws_region         = var.aws_region
  container_image    = var.ecs_container_image
  task_cpu           = var.ecs_task_cpu
  task_memory        = var.ecs_task_memory
  desired_count      = var.app_container_count
  app_port           = var.app_port
  health_check_path  = var.health_check_path
  private_subnet_ids = module.vpc.private_subnet_ids
  security_group_ids = [module.vpc.app_security_group_id]
  target_group_arn   = module.alb.target_group_arn
  secret_arns        = [module.rds.db_secret_arn]
  log_retention_days = var.log_retention_days

  environment_variables = [
    {
      name  = "PORT"
      value = tostring(var.app_port)
    },
    {
      name  = "ENVIRONMENT"
      value = var.environment
    }
  ]

  secret_environment_variables = [
    {
      name      = "DATABASE_URL"
      valueFrom = module.rds.db_secret_arn
    }
  ]
}

# ---------------------------------------------------------------------------
# Automated Backup Storage (Issue #833)
# ---------------------------------------------------------------------------

module "backup" {
  source = "./modules/backup"

  name_prefix    = local.name_prefix
  retention_days = var.backup_retention_days
  force_destroy  = var.backup_force_destroy
}

# ---------------------------------------------------------------------------
# Monitoring & CloudWatch alarms
# ---------------------------------------------------------------------------

module "monitoring" {
  source = "./modules/monitoring"

  name_prefix                 = local.name_prefix
  rds_instance_id             = module.rds.db_instance_id
  alb_arn_suffix              = module.alb.alb_arn_suffix
  target_group_arn_suffix     = module.alb.target_group_arn_suffix
  alarm_actions_arn           = var.alarm_actions_arn
  ok_actions_arn              = var.ok_actions_arn
  rds_cpu_alarm_threshold     = var.rds_cpu_alarm_threshold
  rds_storage_alarm_threshold = var.rds_storage_alarm_threshold
  alb_5xx_alarm_threshold     = var.alb_5xx_alarm_threshold
  alb_latency_alarm_threshold = var.alb_latency_alarm_threshold
  log_retention_days          = var.log_retention_days
}
