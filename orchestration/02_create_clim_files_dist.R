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

workflow_name = "workflow.create.clim.files"

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

  site_era5_path <- normalizePath(workflow_settings$site.era5.path, mustWork = FALSE)
  site_sipnet_met_path <- normalizePath(workflow_settings$site.sipnet.met.path, mustWork = FALSE)
  site_info_filename = workflow_settings$site.info.file
  start_date <- workflow_settings$start.date
  end_date <- workflow_settings$end.date
  num_cores <- workflow_settings$n.workers
  parallel_strategy <- workflow_settings$parallel.strategy
  data_download_directory = file.path(base_workflow_directory, workflow_settings$data.download.reference)
  apptainer_sif = workflow_settings$apptainer$sif
  ensemble_literal <- sprintf(
    "c(%s)",
    paste(sprintf("%sL", seq_len(10)), collapse = ", ")
  )
  tar_option_set(
    packages = c()
  )

  list(
    tar_target(pecan_xml_file, pecan_xml_path, format = "file"),
    tar_target(
      reference_era5_path, 
      reference_external_data_entity(external_workflow_directory=data_download_directory, external_name="data_raw/ERA5_nc", localized_name="ERA5_nc")
    ),
    tar_target(
      site_info_file, 
      reference_external_data_entity(external_workflow_directory=data_download_directory, external_name=site_info_filename, localized_name="site_info.csv")
    ),
    tar_target(
      apptainer_reference, 
      reference_external_data_entity(external_workflow_directory=data_download_directory, external_name=apptainer_sif, localized_name=apptainer_sif)
    ),
    tar_target(pecan_settings, PEcAn.settings::read.settings(pecan_xml_file)),
    tar_target(
      era5_site_combinations,
      build_era5_site_combinations_args(
        site_info_file = site_info_file,
        start_date = start_date,
        end_date = end_date,
        reference_path = reference_era5_path,
        sipnet_met_path = site_sipnet_met_path,
        dependencies = c()
      )
    ),
    tar_target(
      era5_clim_create_args,
      targets_argument_abstraction(
        argument_object = list(
          site_combinations = era5_site_combinations,
          site_era5_path = reference_era5_path,
          site_sipnet_met_path = site_sipnet_met_path,
          n_workers = 1,
          dependencies=c()
        )
      )
    ),
    # tar_target(printed_thing, print(era5_site_combinations)),
    tar_target(
      era5_clim_output,
      targets_abstract_args_sbatch_exec(
        pecan_settings=pecan_settings,
        function_artifact="convert_era5_nc_to_clim", 
        args_artifact="era5_clim_create_args", 
        task_id=uuid::UUIDgenerate(), , 
        apptainer=apptainer_reference, 
        dependencies = era5_clim_create_args,
        functional_source = function_sourcefile
      )
    ),
    tar_target(
      settings_job_outcome,
      pecan_monitor_cluster_job(pecan_settings=pecan_settings, job_id_list=era5_clim_output)
    )
  )
}, ask = FALSE, script = tar_script_path)

script_content <- readLines(tar_script_path)
script_content <- gsub("@FUNCTIONPATH@", workflow_function_path, script_content, fixed = TRUE)
script_content <- gsub("@ORCHESTRATIONXML@", settings_path, script_content, fixed = TRUE)
script_content <- gsub("@WORKFLOWNAME@", workflow_name, script_content, fixed=TRUE)
script_content <- gsub("@PECANXMLPATH@", pecan_config_path, script_content, fixed=TRUE)
writeLines(script_content, tar_script_path)

tar_make(script = tar_script_path)

