variable "region" {
  default = "europe-west1"
}
variable "project" {
  default = "google.com:ml-baguette-demos"
}

variable "location" {
  default = "EU"
}

variable "env" {
  default = "dev_docai_invoices_1"
}

variable "docai_processor" {
  default = "73ed927c221322cf" #Procurement splitter v1 /  #general splitter v1beta3 #"870ff1efce33912a"
}

variable "docai_processor_invoice" {
  default = "13f8400f1d27a89c"
}

variable "source_repository" {
  default = "google.com:ml-baguette-demos/docai-pipeline/master/functions/gcf_input_source/"

}
variable "service_account_email" {
  default = "my-documentai-sa@ml-baguette-demos.google.com.iam.gserviceaccount.com"

}

variable "deletion_protection" {
  default = false
}


provider "google" {
  region = var.region
}

// Big query
resource "google_bigquery_dataset" "dataset_results_docai" {
  dataset_id                  = format("bq_results_doc_%s", var.env)
  friendly_name               = "Invoices results"
  description                 = "Store Document AI results"
  location                    = var.location
//  default_table_expiration_ms = 3600000
  project               = var.project

  labels = {
        env : var.env
  }
}

resource "google_bigquery_table" "results_tables" {
  dataset_id = google_bigquery_dataset.dataset_results_docai.dataset_id
  table_id   = "results"
  project               = var.project

  labels = {
        env : var.env
  }

  schema = <<EOF
[
  {
    "name": "doc_type",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "type of the document"
  },
  {
    "name": "key",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  },
  {
    "name": "value",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  },
  {
    "name": "type",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  },
  {
    "name": "confidence",
    "type": "FLOAT",
    "mode": "NULLABLE",
    "description": "confidence"
  },
  {
    "name": "file",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  }
]
EOF
}


// Storage staging
resource "google_storage_bucket" "gcs_input_doc" {
  name          = format("gcs_input_doc_%s", var.env)
  location      = var.location
  force_destroy = true
  project               = var.project

  uniform_bucket_level_access = true

    labels = {
        "env" : var.env
    }

}

resource "google_storage_bucket" "gcs_json_tmp" {
  name          = format("gcs_json_tmp_%s", var.env)
  location      = var.location
  force_destroy = true
  project               = var.project

  uniform_bucket_level_access = true

    labels = {
        "env" : var.env
    }
}

resource "google_storage_bucket" "gcs_input_single" {
  name          = format("gcs_input_single_%s", var.env)
  location      = var.location
  force_destroy = true
  project               = var.project

  uniform_bucket_level_access = true

    labels = {
        "env" : var.env
    }
}

resource "google_storage_bucket" "gcs_results_json" {
  name          = format("gcs_results_json_%s", var.env)
  location      = var.location
  force_destroy = true
  project               = var.project

  uniform_bucket_level_access = true

    labels = {
        "env" : var.env
    }
}


// Cloud Storage function source archive 
resource "google_storage_bucket" "bucket_source_archives" {
    name          = format("bucket_source_archives_%s", var.env)
    location      = var.location
    force_destroy = true
    project               = var.project

    uniform_bucket_level_access = true

    labels = {
        "env" : var.env
    }
}


// Google Cloud functions

// gcf_input process pdf from input folder

//Source
data "archive_file" "gcf_input_source" {
  type        = "zip"
  source_dir  = "./functions/gcf_input_source"
  output_path = "../tmp/gcf_input_source.zip"
}

resource "google_storage_bucket_object" "gcf_input_source" {
  name   = "gcf_input_source.zip"
  bucket = google_storage_bucket.bucket_source_archives.name
  source = data.archive_file.gcf_input_source.output_path

}


//function 
resource "google_cloudfunctions_function" "gcf_input" {
    name                  = format("gcf_input_%s", var.env)
    description           = "gcf_input process input pdf"
    region = var.region
    project               = var.project

    runtime               = "python39"
    entry_point           = "main_run"
    timeout = 540
    max_instances = 10
    ingress_settings = "ALLOW_INTERNAL_ONLY"
    
    available_memory_mb   = 256
    source_archive_bucket = google_storage_bucket.bucket_source_archives.name 
    source_archive_object = google_storage_bucket_object.gcf_input_source.name

    service_account_email = var.service_account_email  

    event_trigger  {
      event_type    = "google.storage.object.finalize"
      resource=  google_storage_bucket.gcs_input_doc.name 
        failure_policy {
            retry = false
        }
    }

    labels = {
        "env" : var.env
    }

  environment_variables = {
    PROCESSOR = var.docai_processor,
    OUTPUT_URI = google_storage_bucket.gcs_json_tmp.name
  }
}

// gcf_input_single process 1 json file splitted document and call invoice docAI parser

//sources 
data "archive_file" "gcf_input_single_source" {
  type        = "zip"
  source_dir  = "./functions/gcf_input_single_source"
  output_path = "../tmp/gcf_input_single_source.zip"

}

resource "google_storage_bucket_object" "gcf_input_single_source" {
  name   = "gcf_input_single_source.zip"
  bucket = google_storage_bucket.bucket_source_archives.name
  source = data.archive_file.gcf_input_single_source.output_path


}


//function gcf_process_splitter_results
resource "google_cloudfunctions_function" "gcf_process_splitter_results" {
    name                  = format("gcf_process_splitter_results_%s", var.env)
    description           = "gcf_process_splitter_results process splitter json results"
    region = var.region
    project               = var.project

    runtime               = "python39"
    entry_point           = "main_run"
    timeout = 540
    max_instances = 10
    ingress_settings = "ALLOW_INTERNAL_ONLY"
    
    available_memory_mb   = 256
    source_archive_bucket = google_storage_bucket.bucket_source_archives.name 
    source_archive_object = google_storage_bucket_object.gcf_process_splitter_results_source.name

    service_account_email = var.service_account_email  

    event_trigger  {
      event_type    = "google.storage.object.finalize"
      resource=  google_storage_bucket.gcs_json_tmp.name 
        failure_policy {
            retry = false
        }
    }

    labels = {
        "env" : var.env
    }

  environment_variables = {
    PROCESSOR = var.docai_processor,
    OUTPUT_URI = google_storage_bucket.gcs_input_single.name,
    OUTPUT_JSON_URI = google_storage_bucket.gcs_json_tmp.name,
    INPUT_PDF_PATH = google_storage_bucket.gcs_input_doc.name,
    BQ_TABLENAME = format("%s.%s",google_bigquery_table.results_tables.dataset_id, google_bigquery_table.results_tables.table_id)
  }
}


//sources 
data "archive_file" "gcf_process_splitter_results_source" {
  type        = "zip"
  source_dir  = "./functions/gcf_process_splitter_results_source"
  output_path = "../tmp/gcf_process_splitter_results_source.zip"

}

resource "google_storage_bucket_object" "gcf_process_splitter_results_source" {
  name   = "gcf_process_splitter_results_source.zip"
  bucket = google_storage_bucket.bucket_source_archives.name
  source = data.archive_file.gcf_process_splitter_results_source.output_path


}
///// end gcf_process_splitter_results

///// start gcf_parse_results
resource "google_cloudfunctions_function" "gcf_parse_results" {
    name                  = format("gcf_parse_results_%s", var.env)
    description           = "gcf_parse_results process invoice json results"
    region = var.region
    project               = var.project

    runtime               = "python39"
    entry_point           = "main_run"
    timeout = 540
    max_instances = 10
    ingress_settings = "ALLOW_INTERNAL_ONLY"
    
    available_memory_mb   = 256
    source_archive_bucket = google_storage_bucket.bucket_source_archives.name 
    source_archive_object = google_storage_bucket_object.gcf_parse_results_source.name

    service_account_email = var.service_account_email  

    event_trigger  {
      event_type    = "google.storage.object.finalize"
      resource=  google_storage_bucket.gcs_results_json.name 
        failure_policy {
            retry = false
        }
    }

    labels = {
        "env" : var.env
    }

  environment_variables = {
    OUTPUT_JSON_URI = google_storage_bucket.gcs_json_tmp.name,
    INPUT_PDF_PATH = google_storage_bucket.gcs_input_doc.name,
    BQ_TABLENAME = format("%s.invoices_results",  google_bigquery_dataset.dataset_results_docai.dataset_id )

  }
}


//sources 
data "archive_file" "gcf_parse_results_source" {
  type        = "zip"
  source_dir  = "./functions/gcf_parse_results_source"
  output_path = "../tmp/gcf_parse_results_source.zip"

}

resource "google_storage_bucket_object" "gcf_parse_results_source" {
  name   = "gcf_parse_results_source.zip"
  bucket = google_storage_bucket.bucket_source_archives.name
  source = data.archive_file.gcf_parse_results_source.output_path


}
///// end gcf_parse_results_source




//functions 
resource "google_cloudfunctions_function" "gcf_input_single" {
    name                  = format("gcf_input_single_%s", var.env)
    description           = "gcf_input process input pdf"
    region = var.region
    project               = var.project

    runtime               = "python39"
    entry_point           = "main_run"
    timeout = 540
    max_instances = 10
    ingress_settings = "ALLOW_INTERNAL_ONLY"

    available_memory_mb   = 256
    source_archive_bucket = google_storage_bucket.bucket_source_archives.name 
    source_archive_object = google_storage_bucket_object.gcf_input_single_source.name

    service_account_email = var.service_account_email  

    event_trigger  {
      event_type    = "google.storage.object.finalize"
      resource= google_storage_bucket.gcs_input_single.name
          failure_policy {
            retry = false
        }
    }

    labels = {
        "env" : var.env
    }

    environment_variables = {
        PROCESSOR = var.docai_processor_invoice,
        OUTPUT_URI = google_storage_bucket.gcs_results_json.name,
        BQ_TABLENAME = format("%s.%s",google_bigquery_table.results_tables.dataset_id, google_bigquery_table.results_tables.table_id)

    }
}




