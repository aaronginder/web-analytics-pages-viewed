module "pubsub_proc" {
  count                   = var.project_type == "proc" ? 1 : 0
  source                  = "moodule"
  version                 = "v2.0.2"
  stage                   = var.stage
  project_id              = var.project_id
  pubsub_yaml_config_path = var.proc_yaml_config_path
}
