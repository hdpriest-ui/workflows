# How-To: Running the 2a Grass Demo

This guide walks through running the 2a grass example end-to-end using `magic-ensemble`.

## Prerequisites

**Conda environment** at `~/.conda/envs/pecan-all`:
```bash
conda activate ~/.conda/envs/pecan-all
```

**Repository**: clone the main branch and enter the directory:
```bash
git clone https://github.com/ccmmf/workflows.git
cd workflows
```

**AWS CLI** with a profile named `magic` configured for NCSA Garage S3. Verify with:
```bash
aws s3  --profile magic ls s3://carb/
# outputs:
# PRE data/
# PRE data_raw/
# PRE deploy/
# PRE environment-all/
# PRE environments/
# PRE management/
# PRE tmp/
```

**Slurm partition**: if your cluster requires a specific partition, uncomment `slurm_partition` in the config and set it before running Step 3. If omitted, Slurm uses the cluster default.

## Running the workflow

All three commands below must be run from the root of the `workflows` repository.

---

### Step 1: Fetch demo data

```bash
./magic-ensemble get-demo-data --config examples/2a_grass/example_user_config.yaml
```

Creates the run directory and downloads all required input data from S3. This includes
ERA5 meteorology in PEcAn CF format, pre-fetched initial condition source data (soil
carbon from SoilGrids, LAI from MODIS, soil moisture from Copernicus CDS), LandTrendr
aboveground biomass rasters, DWR field boundaries, PFT parameter distributions, and a
demo site list. All data lands inside `example-2a-run-directory/` at the paths the
workflow expects.

**Note**: This step is specific to the demo. For a production run with your own data,
start at Step 2 with a customized config and data staged manually into the run directory.

---

### Step 2: Prepare

```bash
./magic-ensemble prepare-example-2a --config examples/2a_grass/example_user_config.yaml
```

Runs four preparation steps in sequence:

1. **Stage inputs** — copies `site_info.csv` and `template.xml` into the run directory, then patches `template.xml` with the Slurm dispatch block and SIPNET model configuration from the manifest.
2. **ERA5 → SIPNET climate** — converts ERA5 NetCDF files into per-site SIPNET `.clim` driver files, one per met ensemble member, covering the full simulation date range.
3. **Initial conditions** — draws `ic_ensemble_size` samples of soil carbon, soil moisture, LAI, aboveground biomass, and PFT parameters for each site, and writes them as NetCDF initial condition files to `IC_files/`.
4. **XML build** — assembles `settings.xml`, the PEcAn run configuration that wires together the site list, IC files, met files, and model settings for the ensemble run.

---

### Step 3: Run ensembles

```bash
./magic-ensemble run-ensembles --config examples/2a_grass/example_user_config.yaml
```

Invokes PEcAn using `settings.xml` produced by Step 2. PEcAn
submits each ensemble member as an individual Slurm job via `sbatch`, monitors job
status, and waits for all members to complete. Each member runs SIPNET independently
using its own initial conditions and met driver.

Ensemble outputs land in `example-2a-run-directory/output/`.

---

## Configuring a custom run

We will provide detailed guidance on manipulating the config in the future.
