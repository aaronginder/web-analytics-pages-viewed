variable "data_yaml_config_path" {
  type        = string
  description = "(Required) The Path to our YAML config file for data project resources"
}

variable "proc_yaml_config_path" {
  type        = string
  description = "(Required) The Path to our YAML config file for proc project resources"
}


variable "region" {
  type        = string
  default     = "europe-west2"
  description = "(Optional) The regional location for the App Engine"
}

variable "project_type" {
  type        = string
  default     = "data"
  description = "(Optional) Project that contains the resources, proc/ops/data"
}

variable "project_id" {
  type        = string
  default     = ""
  description = "(Required) GCP project name"
}

variable "stage" {
  type        = string
  description = "(Required) GCP project environment"
}

variable "base_job_project_id" {
  type        = string
  description = "host GCP project name for log sink"
}
