variable "project" {
  description = "Project"
  type = string
  default = null
}

variable "region" {
  description = "Region"
  type = string
  default = null
}

variable "location" {
  description = "Project Location"
  type = string
  default = null
}

variable "zone" {
  default = null
  type = string
}

variable "bq_dataset_name" {
  default = null
  type = string
  description = "Dataset Name"
}

variable "gcs_storage" {
  default = null
  type = string
  description = "Storage Bucket Name"
}
