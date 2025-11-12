library(targets)
library(tarchetypes)

get_workflow_args <- function() {
  option_list <- list(
    optparse::make_option(
      c("-s", "--settings"),
      default = NULL,
      type = "character",
      help = "Workflow & PEcAn configuration XML"
    ),
    optparse::make_option(
      "--site_era5_path",
      default = "data_raw/ERA5_nc",
      help = paste(
        "Path to ERA5 NetCDF data in PEcAn CF format, organised as",
        "single-site, single-year files within ensemble-specific subdirectories."
      )
    ),
    optparse::make_option(
      "--site_sipnet_met_path",
      default = "data/ERA5_SIPNET",
      help = paste(
        "Output directory for SIPNET clim files. Results are written to",
        "<site_sipnet_met_path>/<siteid>/ERA5.<ens>.<start>.<end>.clim"
      )
    ),
    optparse::make_option(
      "--site_info_file",
      default = "site_info.csv",
      help = "CSV file with one row per location. Must include an `id` column."
    ),
    optparse::make_option(
      "--start_date",
      default = "2016-01-01",
      help = "Clim file start date (YYYY-MM-DD)."
    ),
    optparse::make_option(
      "--end_date",
      default = "2023-12-31",
      help = "Clim file end date (YYYY-MM-DD)."
    ),
    optparse::make_option(
      "--n_cores",
      default = 1L,
      type = "integer",
      help = "Number of workers to allocate when running the targets pipeline."
    ),
    optparse::make_option(
      "--parallel_strategy",
      default = "multisession",
      help = "Reserved for future parallel execution strategy selections."
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

this_workflow_name <- "workflow.create.clim.files"

workflow_run_directory <- settings$orchestration$workflow.base.run.directory
workflow_settings <- settings$orchestration[[this_workflow_name]]
if (is.null(workflow_settings)) {
  stop(sprintf("Workflow settings for '%s' not found in the configuration XML.", this_workflow_name))
}

workflow_function_source <- settings$orchestration$functions.source
source(workflow_function_source)

function_path <- normalizePath(workflow_function_source)
site_era5_path <- normalizePath(workflow_settings$site.era5.path, mustWork = FALSE)
site_sipnet_met_path <- normalizePath(workflow_settings$site.sipnet.met.path, mustWork = FALSE)
site_info_file <- normalizePath(workflow_settings$site.info.file, mustWork = FALSE)
start_date <- workflow_settings$start.date
end_date <- workflow_settings$end.date
n_cores <- workflow_settings$n.workers
parallel_strategy <- workflow_settings$parallel.strategy

if (!dir.exists(workflow_run_directory)) {
  dir.create(workflow_run_directory, recursive = TRUE)
}
workflow_run_directory <- normalizePath(workflow_run_directory)

ret_obj <- workflow_run_directory_setup(
  run_identifier = workflow_settings$run.identifier,
  workflow_run_directory = workflow_run_directory
)

data_download_path = file.path(workflow_run_directory, workflow_settings$data.download.reference)
apptainer_sif = workflow_settings$apptainer$sif

this_run_directory <- ret_obj$run_dir
run_id <- ret_obj$run_id

message(sprintf("Starting workflow run '%s' in directory: %s", run_id, this_run_directory))

setwd(this_run_directory)
tar_config_set(store = "./")
tar_script_path <- file.path("./executed_pipeline.R")

ensemble_literal <- sprintf(
  "c(%s)",
  paste(sprintf("%sL", seq_len(10)), collapse = ", ")
)

tar_script({
  library(targets)
  library(tarchetypes)
  library(uuid)

  function_sourcefile = "@FUNCTIONPATH@"
  tar_source(function_sourcefile)

  data_download_directory = "@DATADOWNLOADPATH@"
  site_era5_path = "@SITEERA5PATH@"
  site_sipnet_met_path = "@SITESIPNETPATH@"
  site_info_filename = "@SITEINFO@"
  start_date = "@STARTDATE@"
  end_date = "@ENDDATE@"
  ensemble_members = as.integer("@ENSEMBLE_MEMBERS@")
  apptainer_sif = "@APPTAINERSIF@"
  num_cores = "@NUMBEROFCORES@"

  tar_option_set(
    packages = c()
  )

  list(
    
    tar_target(reference_era5_path, reference_external_data_entity(external_workflow_directory=data_download_directory, external_name="data_raw/ERA5_nc", localized_name="ERA5_nc")),
    tar_target(site_info_file, reference_external_data_entity(external_workflow_directory=data_download_directory, external_name=site_info_filename, localized_name="site_info.csv")),
    tar_target(
      apptainer_reference, 
      reference_external_data_entity(
        external_workflow_directory=data_download_directory, 
        external_name=apptainer_sif, 
        localized_name=apptainer_sif
      )
    ),
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
          n_workers = num_cores,
          dependencies=c()
        )
      )
    ),
    # tar_target(printed_thing, print(era5_site_combinations)),
    tar_target(
      era5_clim_output,
      targets_based_sourced_containerized_local_exec(
        function_artifact="convert_era5_nc_to_clim", 
        args_artifact="era5_clim_create_args", 
        task_id=uuid::UUIDgenerate(), , 
        apptainer=apptainer_reference, 
        dependencies = era5_clim_create_args,
        functional_source = function_sourcefile
      )
    )
  )
}, ask = FALSE, script = tar_script_path)

script_content <- readLines(tar_script_path)
script_content <- gsub("@FUNCTIONPATH@", function_path, script_content, fixed = TRUE)
script_content <- gsub("@DATADOWNLOADPATH@", data_download_path, script_content, fixed = TRUE)
script_content <- gsub("@SITEERA5PATH@", site_era5_path, script_content, fixed = TRUE)
script_content <- gsub("@SITESIPNETPATH@", site_sipnet_met_path, script_content, fixed = TRUE)
script_content <- gsub("@SITEINFO@", site_info_file, script_content, fixed = TRUE)
script_content <- gsub("@STARTDATE@", start_date, script_content, fixed = TRUE)
script_content <- gsub("@ENDDATE@", end_date, script_content, fixed = TRUE)
script_content <- gsub("@ENSEMBLE_MEMBERS@", ensemble_literal, script_content, fixed = TRUE)
script_content <- gsub("@NUMBEROFCORES@", as.character(n_cores), script_content, fixed = TRUE)
script_content <- gsub("@APPTAINERSIF@", apptainer_sif, script_content)
writeLines(script_content, tar_script_path)

tar_make(script = tar_script_path)

