

variable "project_id" {
  description = "GCP Project ID"
  type        = string
  default     = "nike-data-platform"
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "us-central1"
}

variable "location" {
  description = "Project Location"
  type        = string
  default     = "US"
}

variable "bq_dataset_id" {
  description = "BigQuery Dataset ID"
  type        = string
  default     = "nike_dw"
}

variable "gcs_bucket_name" {
  description = "GCS Bucket Name"
  type        = string
  default     = "nike-data-lake"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  type        = string
  default     = "STANDARD"
}
