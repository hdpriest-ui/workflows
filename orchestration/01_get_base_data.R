library(targets)
library(tarchetypes)

get_workflow_args <- function() {
  option_list <- list(
    optparse::make_option(
      c("-s", "--settings"),
      default = NULL,
      type = "character",
      help = "Workflow configuration XML"
    )
  )

  parser <- optparse::OptionParser(option_list = option_list)
  optparse::parse_args(parser)
}

args <- get_workflow_args()

if (is.null(args$settings)) {
  stop("A PEcAn settings XML must be provided via --settings.")
}

settings <- PEcAn.settings::read.settings(args$settings)

this_workflow_name <- "workflow.get.base.data"

workflow_run_directory <- settings$orchestration$workflow.base.run.directory
workflow_settings <- settings$orchestration[[this_workflow_name]]
if (is.null(workflow_settings)) {
  stop(sprintf("Workflow settings for '%s' not found in the configuration XML.", this_workflow_name))
}

workflow_function_source <- settings$orchestration$functions.source
source(workflow_function_source)

function_path <- normalizePath(workflow_function_source)

if (!dir.exists(workflow_run_directory)) {
  dir.create(workflow_run_directory, recursive = TRUE)
}
workflow_run_directory <- normalizePath(workflow_run_directory)

artifact1_url <- workflow_settings[["ccmmf.s3.artifact.01.url"]]
artifact1_filename <- workflow_settings[["ccmmf.s3.artifact.01.filename"]]
artifact2_url <- workflow_settings[["ccmmf.s3.artifact.02.url"]]
artifact2_filename <- workflow_settings[["ccmmf.s3.artifact.02.filename"]]

if (any(vapply(
  list(artifact1_url, artifact1_filename, artifact2_url, artifact2_filename),
  is.null,
  logical(1)
))) {
  stop("workflow.get.base.data must define ccmmf.s3.artifact.01/02 url and filename entries.")
}

ret_obj <- workflow_run_directory_setup(
  run_identifier = workflow_settings$run.identifier,
  workflow_run_directory = workflow_run_directory
)

this_run_directory <- ret_obj$run_dir
run_id <- ret_obj$run_id

message(sprintf("Starting workflow run '%s' in directory: %s", run_id, this_run_directory))

setwd(this_run_directory)
tar_config_set(store = "./")
tar_script_path <- file.path("./executed_get_base_data.R")

tar_script({
  library(targets)
  library(tarchetypes)

  tar_source("@FUNCTIONPATH@")
  apptainer_url = "@APPTAINERURL"
  apptainer_name = "@APPTAINERNAME@"
  apptainer_tag = "@APPTAINERTAG@"
  apptainer_sif = "@APPTAINERSIF@"
  tar_option_set(packages = character(0))

  list(
    tar_target(
      ccmmf_artifact_01_file,
      download_ccmmf_data(
        prefix_url = "@ARTIFACT1_URL@",
        local_path = tar_path_store(),
        prefix_filename = "@ARTIFACT1_FILENAME@"
      )
    ),
    tar_target(
      ccmmf_artifact_01_contents,
      untar(ccmmf_artifact_01_file, exdir = tar_path_store())
    ),
    tar_target(
      ccmmf_artifact_02_file,
      download_ccmmf_data(
        prefix_url = "@ARTIFACT2_URL@",
        local_path = tar_path_store(),
        prefix_filename = "@ARTIFACT2_FILENAME@"
      )
    ),
    tar_target(
      ccmmf_artifact_02_contents,
      untar(ccmmf_artifact_02_file, exdir = tar_path_store())
    ),
    tar_target(
      apptainer_reference, 
      pull_apptainer_container(
        apptainer_url_base=apptainer_url, 
        apptainer_image_name=apptainer_name, 
        apptainer_tag=apptainer_tag, 
        apptainer_disk_sif=apptainer_sif
      )
    )
  )
}, ask = FALSE, script = tar_script_path)

script_content <- readLines(tar_script_path)
script_content <- gsub("@FUNCTIONPATH@", function_path, script_content, fixed = TRUE)
script_content <- gsub("@ARTIFACT1_URL@", artifact1_url, script_content, fixed = TRUE)
script_content <- gsub("@ARTIFACT1_FILENAME@", artifact1_filename, script_content, fixed = TRUE)
script_content <- gsub("@ARTIFACT2_URL@", artifact2_url, script_content, fixed = TRUE)
script_content <- gsub("@ARTIFACT2_FILENAME@", artifact2_filename, script_content, fixed = TRUE)
script_content <- gsub("@APPTAINERURL", workflow_settings$apptainer$remote.url, script_content)
script_content <- gsub("@APPTAINERNAME@", workflow_settings$apptainer$container.name, script_content)
script_content <- gsub("@APPTAINERTAG@", workflow_settings$apptainer$tag, script_content)
script_content <- gsub("@APPTAINERSIF@", workflow_settings$apptainer$sif, script_content)

writeLines(script_content, tar_script_path)

tar_make(script = tar_script_path)

