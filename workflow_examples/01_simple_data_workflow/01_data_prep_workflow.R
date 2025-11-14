library(targets)
library(tarchetypes)
library(PEcAn.settings)

get_workflow_args <- function() {
  option_list <- list(
    optparse::make_option(
      c("-s", "--settings"),
      default = NULL,
      type = "character",
      help = "Workflow & Pecan configuration XML",
    )
  )

  parser <- optparse::OptionParser(option_list = option_list)
  args <- optparse::parse_args(parser)

  return(args)
}


args <- get_workflow_args()

if (is.null(args$settings)) {
  stop("An Orchestration settings XML must be provided via --settings.")
}
# note: if this_run_directory exists already, and we specify the _targets script within it, targets will evaluate the pipeline already run
# if the pipeline has not changed, the pipeline will not run. This extends to the targeted functions, their arguments, and their arguments values. 
# thus, as long as the components of the pipeline run are kept in the functions, the data entities, and the arguments, we can have smart re-evaluation.

workflow_name = "workflow.data.prep.1"

settings_path = normalizePath(file.path(args$settings))
settings = XML::xmlToList(XML::xmlParse(args$settings))

workflow_function_source = file.path(settings$orchestration$functions.source)
workflow_function_path = normalizePath(workflow_function_source)
source(workflow_function_source)

# hopefully can find a more elegant way to do this
pecan_config_path = normalizePath(file.path(settings$orchestration[[workflow_name]]$pecan.xml.path))

ret_obj <- workflow_run_directory_setup(orchestration_settings=settings, workflow_name=workflow_name)

analysis_run_directory = ret_obj$run_dir
run_id = ret_obj$run_id

message(sprintf("Starting workflow run '%s' in directory: %s", run_id, analysis_run_directory))

setwd(analysis_run_directory)
tar_config_set(store = "./")
tar_script_path <- file.path("./executed_pipeline.R")

#### Pipeline definition ####
tar_script({
  library(targets)
  library(tarchetypes)
  library(uuid)
  library(XML)

  function_sourcefile = "@FUNCTIONPATH@"
  tar_source(function_sourcefile)

  orchestration_settings = parse_orchestration_xml("@ORCHESTRATIONXML@")
  pecan_xml_path = "@PECANXMLPATH@"
  workflow_name = "@WORKFLOWNAME@"
  workflow_settings = orchestration_settings$orchestration[[workflow_name]]
  base_workflow_directory = orchestration_settings$orchestration$workflow.base.run.directory
  if (is.null(workflow_settings)) {
    stop(sprintf("Workflow settings for '%s' not found in the configuration XML.", this_workflow_name))
  }

  ccmmf_data_tarball_url = workflow_settings$ccmmf.data.s3.url
  ccmmf_data_filename = workflow_settings$ccmmf.data.tarball.filename
  run_identifier = workflow_settings$run.identifier

  tar_option_set(
    packages = c("readr", "dplyr"),
    imports = c()
  )
  list(
    # source data handling
    tar_target(ccmmf_data_tarball, download_ccmmf_data(prefix_url=ccmmf_data_tarball_url, local_path=tar_path_store(), prefix_filename=ccmmf_data_filename)),
    tar_target(workflow_data_paths, untar(ccmmf_data_tarball, exdir = tar_path_store())),
    tar_target(obtained_resources_untar, untar(ccmmf_data_tarball, list = TRUE))
  )
}, ask = FALSE, script = tar_script_path)

# because tar_make executes the script in a separate process based on the created workflow directory,
# in order to parametrize the workflow script, we have to first create placeholders, and then below, replace them with actual values.
# if we simply place the variables in the script definition above, they are evaluated as the time the script is executed by tar_make()
# that execution takes place in a different process + memory space, in which those variables are not accessible.
# so, we create the execution script, and then text-edit in the parameters.
# Read the generated script and replace placeholders with actual file paths
script_content <- readLines(tar_script_path)
script_content <- gsub("@FUNCTIONPATH@", workflow_function_path, script_content, fixed = TRUE)
script_content <- gsub("@ORCHESTRATIONXML@", settings_path, script_content, fixed = TRUE)
script_content <- gsub("@WORKFLOWNAME@", workflow_name, script_content, fixed=TRUE)
script_content <- gsub("@PECANXMLPATH@", pecan_config_path, script_content, fixed=TRUE)
writeLines(script_content, tar_script_path)

#### workflow execution ####
# this changes the cwd to the designated tar store
tar_make(script = tar_script_path)
