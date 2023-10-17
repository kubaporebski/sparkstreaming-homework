variable "project" {
	description = "ID of your GCP project"
	type = string
	default = "jakubporebski-proj"
}

variable "location" {
	description = "Location of GCS bucket"
	type = string
	default = "EU"
}

variable "region" {
	description = "Region for deployment"
	type = string
	default = "europe-central2"
}

variable "zone" {
	description = "Zone in the region for deployment"
	type = string
	default = "europe-central2-a"
}