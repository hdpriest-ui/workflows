# this needs to be at the top, what version are we building
ARG IMAGE_VERSION="develop"
ARG PARENT_IMAGE="pecan/model-sipnet-git"

FROM ${PARENT_IMAGE}:${IMAGE_VERSION}
# ----------------------------------------------------------------------
# ADD IN TARGETS FOR CCMMF NEEDS
# ----------------------------------------------------------------------

RUN Rscript --vanilla -e "install.packages(c('targets', 'uuid', 'tarchetypes'), repos = c(CRAN = 'cloud.r-project.org'))"