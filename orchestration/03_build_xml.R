library(targets)
library(tarchetypes)
library(XML)

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
  stop("An Orchestration settings XML must be provided via --settings.")
}

workflow_name = "workflow.build.xml"

settings_path = normalizePath(file.path(args$settings))
settings = XML::xmlToList(XML::xmlParse(args$settings))

workflow_function_source = file.path(settings$orchestration$functions.source)
workflow_function_path = normalizePath(workflow_function_source)
source(workflow_function_source)

# hopefully can find a more elegant way to do this
pecan_template_path = normalizePath(file.path(settings$orchestration[[workflow_name]]$pecan.xml.template))

ret_obj <- workflow_run_directory_setup(orchestration_settings=settings, workflow_name=workflow_name)

analysis_run_directory = ret_obj$run_dir
run_id = ret_obj$run_id

message(sprintf("Starting workflow run '%s' in directory: %s", run_id, analysis_run_directory))

setwd(analysis_run_directory)
tar_config_set(store = "./")
tar_script_path <- file.path("./executed_pipeline.R")

tar_script({
  library(targets)
  library(tarchetypes)
  library(uuid)
  library(XML)

  function_sourcefile = "@FUNCTIONPATH@"
  tar_source(function_sourcefile)

  orchestration_settings = parse_orchestration_xml("@ORCHESTRATIONXML@")
  pecan_template_path = "@PECANTEMPLATEPATH@"
  workflow_name = "@WORKFLOWNAME@"
  workflow_settings = orchestration_settings$orchestration[[workflow_name]]
  base_workflow_directory = orchestration_settings$orchestration$workflow.base.run.directory
  if (is.null(workflow_settings)) {
    stop(sprintf("Workflow settings for '%s' not found in the configuration XML.", this_workflow_name))
  }

  site_info_filename = workflow_settings$site.info.file
  start_date <- workflow_settings$start.date
  end_date <- workflow_settings$end.date
  data_download_directory = file.path(base_workflow_directory, workflow_settings$data.download.reference)

  tar_option_set(
    packages = c()
  )

  list(
    tar_target(pecan_template_file, pecan_template_path, format = "file"),
    tar_target(
      reference_era5_path, 
      reference_external_data_entity(external_workflow_directory=data_download_directory, external_name="data_raw/ERA5_nc", localized_name="ERA5_nc")
    ),
    tar_target(
      site_info_file, 
      reference_external_data_entity(external_workflow_directory=data_download_directory, external_name=site_info_filename, localized_name="site_info.csv")
    ),
    tar_target(
      IC_files, 
      reference_external_data_entity(external_workflow_directory=data_download_directory, external_name="IC_files", localized_name="IC_files")
    ),
    tar_target(
        built_xml,
        build_pecan_xml(orchestration_xml=workflow_settings, template_file=pecan_template_file, dependencies=c(reference_era5_path, site_info_file, IC_files))
    )
    
  )
}, ask = FALSE, script = tar_script_path)

script_content <- readLines(tar_script_path)
script_content <- gsub("@FUNCTIONPATH@", workflow_function_path, script_content, fixed = TRUE)
script_content <- gsub("@ORCHESTRATIONXML@", settings_path, script_content, fixed = TRUE)
script_content <- gsub("@WORKFLOWNAME@", workflow_name, script_content, fixed=TRUE)
script_content <- gsub("@PECANTEMPLATEPATH@", pecan_template_path, script_content, fixed=TRUE)
writeLines(script_content, tar_script_path)

tar_make(script = tar_script_path)

