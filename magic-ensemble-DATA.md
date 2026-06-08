# magic-ensemble: Data Sources and Routing

This document describes every data input required for a production `prepare` run:
where the data originates, what form it takes, and how it enters the workflow.

---

## Summary table

| Input | Origin | How it enters the workflow | `external_paths` staging |
|-------|--------|---------------------------|--------------------------|
| Site list | User-defined | `00_stage_external_inputs.sh` copies from `external_paths.site_info_file` to `run_dir/site_info.csv` | Yes |
| PEcAn XML template | User-defined | `00_stage_external_inputs.sh` copies from `external_paths.template_file` to `run_dir/template.xml` | Yes |
| ERA5 meteorology | Copernicus ERA5 reanalysis, via PEcAn met pipeline | Extracted from `s3://carb/data_raw/ensembles_data_artifact.tar.gz` by `00_fetch_s3_and_prepare_run_dir.sh` | No — directory |
| DWR field boundaries | California DWR Crop Mapping 2018 | Extracted from `s3://carb/data_raw/ensembles_data_artifact.tar.gz` by `00_fetch_s3_and_prepare_run_dir.sh` | No — directory |
| PFT parameter distributions | PEcAn calibration runs | Extracted from `s3://carb/data_raw/ensembles_data_artifact.tar.gz` by `00_fetch_s3_and_prepare_run_dir.sh` | No — directory |
| LandTrendr aboveground biomass | Kennedy lab / Oregon State CEOAS | Downloaded individually from `s3://carb/data_raw/` by `00_fetch_s3_and_prepare_run_dir.sh` | No — comma-separated multi-file key |
| Soil organic carbon | SoilGrids 250m | Extracted from `s3://carb/data_raw/ensembles_data_artifact.tar.gz` by `00_fetch_s3_and_prepare_run_dir.sh` | No — directory |
| Leaf area index | MODIS | Extracted from `s3://carb/data_raw/ensembles_data_artifact.tar.gz` by `00_fetch_s3_and_prepare_run_dir.sh` | No — directory |
| Soil moisture | Copernicus CDS | Downloaded and extracted from `s3://carb/data_raw/moisture_20160101_20160110.tgz` by `00_fetch_s3_and_prepare_run_dir.sh` | No — directory |

---

## Inputs staged via `external_paths`

These are provided by the user in their config and copied into the run directory
before `prepare` runs. Each key maps to a fixed location inside `run_dir` defined
by the workflow manifest.

### Site list (`site_info_file`)

User-defined CSV with one row per modeled site. Required columns: `id`, `name`,
`lat`, `lon`, `field_id` (matches `UniqueID` in the DWR geodatabase), `site.pft`
(matches a subdirectory name in `pft_dir`).

Consumed by: `prepare` steps 1 and 2.

### PEcAn XML template (`template_file`)

User-defined XML configuration template for PEcAn. During `prepare` step 0, the
CLI patches this file with the `<host>` dispatch block selected by `pecan_dispatch`
and the `<model>` block from the manifest before passing it to step 3
(`03_xml_build.R`). Use `examples/2a_grass/template.xml` as a starting point.

Consumed by: `prepare` step 3.

### ERA5 meteorology (`site_era5_path`)

ERA5 reanalysis meteorology in PEcAn CF format (NetCDF), produced upstream by a
separate PEcAn met processing workflow (not part of `magic-ensemble`). Must cover
the full simulation date range. Files are organized as one subdirectory per
ERA5 half-degree grid cell per met ensemble member:

```
site_era5_path/
  ERA5_38.5N_121.5W_1/
    ERA5.1.2016.nc
    ERA5.1.2017.nc
    ...
  ERA5_38.5N_121.5W_2/
    ...
```

`prepare` step 1 (`01_ERA5_nc_to_clim.R`) converts these NetCDFs into per-site
SIPNET `.clim` driver files and writes them to `data/ERA5_SIPNET/`.

Consumed by: `prepare` step 1.

### DWR field boundaries (`field_shape_path`)

California Department of Water Resources i15 Crop Mapping 2018 geodatabase
(`.gdb`). A static statewide reference dataset — downloaded once and reused across
runs. Must include a `UniqueID` field that matches the `field_id` column in the
site list.

Used by `prepare` step 2 to extract per-field aboveground biomass from the
LandTrendr rasters.

Consumed by: `prepare` step 2.

### PFT parameter distributions (`pft_dir`)

Output of prior PEcAn parameter calibration runs. One subdirectory per PFT, named
to match values in the `site.pft` column of the site list. Each subdirectory must
contain `post.distns.Rdata` — the posterior parameter distribution produced by
PEcAn's parameter data assimilation.

Used by `prepare` step 2 to sample initial condition parameters (SLA, leaf carbon
fraction, wood carbon fraction) for each site.

Consumed by: `prepare` step 2.

### LandTrendr aboveground biomass (`landtrendr_raw_files`)

Two 30 m GeoTIFFs of aboveground biomass (clipped to California) from the Kennedy
lab at Oregon State (CEOAS). Originally distributed via FTP; the versions used here
are stored in S3 (`s3://carb/data_raw/`).

- `ca_biomassfiaald_2016_median.tif` — median AGB estimate (Mg/ha)
- `ca_biomassfiaald_2016_stdv.tif` — standard deviation of AGB estimate

Used by `prepare` step 2 to extract per-field initial aboveground biomass estimates.

Consumed by: `prepare` step 2.

---

## Inputs fetched at runtime and cached

These are retrieved by `prepare` step 2 (`02_ic_build.R`) via API on the first run
for a given set of sites. Results are written to `data/IC_prep/` and reused on
all subsequent runs — the script checks for the cache file before making any
outbound request.

In practice, for a statewide production run this data is pre-staged (either from
a prior run or from the S3 artifact) so no API calls occur.

### Soil organic carbon

**Source:** SoilGrids 250m (`PEcAn.data.land::soilgrids_soilC_extract`)  
**Cache:** `data/IC_prep/soilgrids_soilC_data.csv`  
Values: mean and SD of total soil carbon (0–30 cm depth) per site.

### Leaf area index

**Source:** MODIS (`PEcAn.data.remote::MODIS_LAI_prep`)  
**Cache:** `data/IC_prep/LAI_bysite.csv`  
Values: site-level LAI near `run_LAI_date` (±30-day search window).

### Soil moisture

**Source:** Copernicus Climate Data Store (`PEcAn.data.land::extract_SM_CDS`)  
**Cache:** `data/IC_prep/sm.csv`  
Values: mean and uncertainty of volumetric soil moisture fraction at `start_date`.

---

## How `prepare` step 2 assembles initial conditions

After loading or fetching all of the above, `02_ic_build.R` assembles per-site
estimates of four state variables:

| Variable | Source |
|----------|--------|
| Soil organic carbon | SoilGrids |
| Soil moisture fraction | Copernicus CDS |
| Leaf area index | MODIS |
| Aboveground biomass | LandTrendr rasters + DWR field boundaries |

These are combined with PFT parameter samples drawn from `post.distns.Rdata` to
produce an ensemble of `ic_ensemble_size` initial condition NetCDF files per site,
written to `IC_files/`. Those files are then consumed by step 3 (`03_xml_build.R`)
to build `settings.xml` for the ensemble run.
