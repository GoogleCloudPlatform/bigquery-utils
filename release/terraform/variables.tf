variable "project" { 
    type = string
}

variable "bq_regions" {
    type    = list(string)
    default = [
        "eu",
        "us-east5",
        "us-south1",
        "us-central1",
        "us-west4",
        "us-west2",
        "northamerica-northeast1",
        "us-east4",
        "us-west1",
        "us-west3",
        "southamerica-east1",
        "southamerica-west1",
        "us-east1",
        "northamerica-northeast2",
        "asia-south2",
        "asia-east2",
        "asia-southeast2",
        "australia-southeast2",
        "asia-south1",
        "asia-northeast2",
        "asia-northeast3",
        "asia-southeast1",
        "australia-southeast1",
        "asia-east1",
        "asia-northeast1",
        "europe-west1",
        "europe-west10",
        "europe-north1",
        "europe-west3",
        "europe-west2",
        "europe-southwest1",
        "europe-west8",
        "europe-west4",
        "europe-west9",
        "europe-west12",
        "europe-central2",
        "europe-west6",
        "me-central2",
        "me-central1",
        "me-west1",
        "africa-south1"
    ]
}
