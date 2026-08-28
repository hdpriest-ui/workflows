#!/usr/bin/env Rscript

# --------------------------------------------------
# Run-time parameters

options <- list(
  optparse::make_option(c("-s", "--settings"),
    default = "settings.xml",
    help = paste(
      "path to the XML settings file you want to use for this run.",
      "Be aware all paths inside the file are interpreted relative to the",
      "working directory of the process that invokes run_model.R,",
      "not relative to the settings file path"
    )
  )
) |>
  # Show default values in help message
  purrr::modify(\(x) {
    x@help <- paste(x@help, "[default: %default]")
    x
  })

args <- optparse::OptionParser(option_list = options) |>
  optparse::parse_args()




# make sure always to call status.end
options(warn = 1)
options(error = quote({
  try(PEcAn.utils::status.end("ERROR"))
  try(PEcAn.remote::kill.tunnel(settings))
  if (!interactive()) {
    q(status = 1)
  }
}))

# ----------------------------------------------------------------------
# PEcAn Workflow
# ----------------------------------------------------------------------

library("PEcAn.all")

# --- TEMPORARY UPSTREAM PATCH --------------------------------------------
# PEcAn.SIPNET::write.config.SIPNET() computes its local rundir/outdir as
# settings$host$rundir/host$outdir UNLESS (settings$host$qsub is unset AND
# settings$host$name == "localhost"). For us, host$qsub IS set (slurm-sbatch
# dispatch) even though we always run locally, so that override never fires
# and rundir/outdir fall back to settings$host$rundir/host$outdir. For plain
# per-run configs this happens to be harmless (settings$rundir and
# settings$host$rundir are the same absolute path in our settings.xml), but
# PEcAn.SIPNET::write_segment_configs() (used by write_segmented_configs.SIPNET,
# the CONFIG_SEGMENTS step below) only overrides settings$rundir/modeloutdir
# to redirect a segment's config into its own nested directory -- it never
# touches settings$host$rundir/host$outdir, which stay pointed at the
# top-level run directory. Combined with write_segment_configs()'s
# intentionally-reused dummy run.id "1" (see
# PEcAn.SIPNET:::write_segment_configs), every segment's rundir/outdir
# computation lands on the wrong, nonexistent "output/run/1"/"output/out/1"
# instead of the correct nested segment path -- breaking the events.in copy
# (fails loudly, a Warning) and, more importantly, the @RUNDIR@/@OUTDIR@
# substitution baked into each segment's own job.sh content (fails silently).
#
# The qsub check is the wrong discriminator here: it answers "how are
# ensemble members dispatched," not "where do config files live on this
# host" -- for any localhost run (dispatched via qsub or not), rundir/outdir
# should always come from settings$rundir/modeloutdir, which every caller
# (segmented or not) already correctly controls. This patches that condition
# in-memory only (does not touch the installed package on disk -- same
# mechanism trace() below uses, via assignInNamespace); it needs a real
# upstream PEcAn.SIPNET fix. Remove this block once that lands.
local({
  fn <- PEcAn.SIPNET::write.config.SIPNET
  b <- body(fn)
  target <- 20L
  stopifnot(
    identical(
      deparse(b[[target]][[2]]),
      'is.null(settings$host$qsub) && (settings$host$name == "localhost")'
    )
  )
  new_stmt <- b[[target]]
  new_stmt[[2]] <- quote(settings$host$name == "localhost")
  b[[target]] <- new_stmt
  body(fn) <- b
  assignInNamespace("write.config.SIPNET", fn, ns = "PEcAn.SIPNET")
})
# --- END TEMPORARY UPSTREAM PATCH -----------------------------------------

# --- TEMPORARY DIAGNOSTIC TRACE ---------------------------------------
# Instruments PEcAn.SIPNET::write.config.SIPNET() in-session only (does
# not touch the installed package) to log, for every call, the run.id it
# receives and the PFT set in trait.values -- checking whether per-site
# PFT narrowing (settings$run$site$site.pft) actually takes effect, and
# whether run.id is ever something other than a real ENS-XXXXX-<site> id
# (relates to the "output/run/1/events.in" and multi-PFT warning
# investigations). Remove once resolved.
trace(PEcAn.SIPNET::write.config.SIPNET, tracer = quote({
  cat(sprintf(
    "[TRACE write.config.SIPNET] run.id=%s (class=%s) site.id=%s pfts=[%s] n_pfts=%d\n",
    as.character(run.id),
    class(run.id),
    tryCatch(as.character(settings$run$site$id), error = function(e) "NA"),
    paste(names(trait.values), collapse = ","),
    length(trait.values)
  ))
}), print = FALSE)

# Instruments the internal (unexported) PEcAn.SIPNET:::write_segment_configs
# right after its crop_code -> pft mapping step (body statement 14, "segments
# <- dplyr::mutate(pft = dplyr::coalesce(...), ...)") to catch, per site, the
# full crop_code/pft table as computed -- checking whether an NA pft (seen
# for site 515023) already exists at this point (bug in segment_dataframe()/
# crop2pft_example()) or is introduced later, in the per-segment loop's
# choose_pft/run_traits indexing. Remove once resolved.
trace(PEcAn.SIPNET:::write_segment_configs, at = 15, tracer = quote({
  cat("[TRACE write_segment_configs] site=", run_settings$run$site$id, "\n")
  print(segments[, c("crop_code", "pft")])
}), print = FALSE)
# --- END TEMPORARY DIAGNOSTIC TRACE -------------------------------------

# Report package versions for provenance
PEcAn.all::pecan_version()

# Open and read in settings file for PEcAn run.
settings <- PEcAn.settings::read.settings(args$settings)

if (!dir.exists(settings$outdir)) {
  dir.create(settings$outdir, recursive = TRUE)
}
PEcAn.logger::logger.setLevel("WARN")

PEcAn.utils::status.start("DESIGN")
ens_design <- PEcAn.uncertainty::generate_joint_ensemble_design(
  settings = settings[[1]],
  ensemble_size = settings$ensemble$size
)
write.csv(ens_design$X, file.path(settings$outdir, "input_design.csv"))
sample_env <- list2env(ens_design$samples)
save(
  list = ls(sample_env),
  envir = sample_env,
  file = file.path(settings$outdir, "samples.Rdata")
)

settings$ensemble$id <- rlang::hash(ens_design)
PEcAn.utils::status.end()

# Write model specific configs
if (PEcAn.utils::status.check("CONFIG") == 0) {
  PEcAn.utils::status.start("CONFIG")
  settings <- PEcAn.workflow::runModule.run.write.configs(
    settings,
    input_design = ens_design
  )
  PEcAn.settings::write.settings(settings, outputfile = "pecan.CONFIGS.xml")
  PEcAn.utils::status.end()
}

PEcAn.utils::status.start("CONFIG_SEGMENTS")
run_script_paths <- papply(
  settings,
  \(s) PEcAn.SIPNET::write_segmented_configs.SIPNET(s, ens_design$X)
)
PEcAn.utils::status.end()
