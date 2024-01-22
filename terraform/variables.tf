variable "creds" {
  description = "Credentials"
  default     = "keys/creds.json"
}

variable "project" {
  description = "Project"
  default     = "helical-casing-411709"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "region" {
  description = "Project Region"
  default     = "us-central1"
}

variable "bq_dataset_name" {
  description = "My BQ Dataset Name"
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "My GCP Bucket Name"
  default     = "helical-casing-411709-terra-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}