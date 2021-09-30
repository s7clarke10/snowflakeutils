
# snowflakeutils <a href="https://github/s7clarke10/snowflakeutils"><img src="https://raw.githubusercontent.com/s7clarke10/snowflakeutils/master/.graphics/logo.png" align="right" height="140" /></a>

<!-- badges: start -->
[![CRAN status](https://cranchecks.info/badges/flavor/release/snowflakeutils)](https://cran.r-project.org/web/checks/check_results_snowflakeutils.html)
[![R-CMD-check](https://github.com/s7clarke10/snowflakeutils/workflows/R-CMD-check/badge.svg)](https://github.com/s7clarke10/snowflakeutils/actions)
[![AppVeyor build status](https://ci.appveyor.com/api/projects/status/kayjdh5qtgymhoxr/branch/master?svg=true)](https://ci.appveyor.com/project/s7clarke10/snowflakeutils)
[![Codecov test coverage](https://codecov.io/github/s7clarke10/snowflakeutils/coverage.svg?branch=master)](https://codecov.io/github/s7clarke10/snowflakeutils?branch=master)
[![GitLab CI build status](https://gitlab.com/s7clarke10/snowflakeutils/badges/master/pipeline.svg)](https://gitlab.com/s7clarke10/snowflakeutils/-/pipelines)
[![downloads](https://cranlogs.r-pkg.org/badges/snowflakeutils)](https://www.rdocumentation.org/trends)
[![CRAN usage](https://jangorecki.gitlab.io/rdeps/snowflakeutils/CRAN_usage.svg?sanitize=true)](https://gitlab.com/jangorecki/rdeps)
[![BioC usage](https://jangorecki.gitlab.io/rdeps/snowflakeutils/BioC_usage.svg?sanitize=true)](https://gitlab.com/jangorecki/rdeps)
[![indirect usage](https://jangorecki.gitlab.io/rdeps/snowflakeutils/indirect_usage.svg?sanitize=true)](https://gitlab.com/jangorecki/rdeps)
<!-- badges: end -->

`snowflakeutils` provides a high-performance version for writing https://www.r-project.org/about.html)'s `data.frame` to Snowflake for ease of use, convenience and programming speed.

## Why `snowflakeutils`?

* Simple to use
* fast write speed
* Append and Overwrite functionality
* Snowflake Stage and Format management

## Installation Options

```r
# Standard R Installation
install.packages("snowflakeutils")

# Install using GitHub
install_github("s7clarke10/snowflakeutils")

# Installation using devtools
devtools::install()

# To install to a specific library using devtools
withr::with_libpaths(new='libPath', devtools::install() )
```

## Usage

This write_to_snowflake function assume a connection is already established to Snowflake. Supply the connection object, dataframe to write_to_snowflake, and the name of the target Snowflake table.

The function will on a temporary basis stage the file you wish to load into Snowflake into temp location indicated by the tempdir() function. Please ensure your temp directory is sized appropriately for large datasets.
This file is then put to an internal Snowflake Stage and then copied into Snowflake. This generally performs a lot fast than direct ODBC write commands via R DBI / ODBC.

```r
library(datasets)
data(iris)

library(DBI)
library(odbc)

# snowflakeutils used to write to Snowflake
library(snowflakeutils)


#### < INSERT LOGIC to connect to Snowflake using DBI and ODBC > ####

# Example load IRIS dataset to Snowflake as TEST_IRIS_TABLE.
target_table  <- paste0("<Target Snowflake DB>.<Target Snowflake Schema>.TEST_IRIS_TABLE")
snowflake_put_copy_time <- write_to_snowflake(con, iris, target_table, overwrite=TRUE, print_messages = TRUE)
```

NOTE: Please see the Package Description for more detailed usage, setup and use of parameters.

### Stay up-to-date

- Initial Release

### Contributing

Guidelines for filing issues / pull requests: Provide evidence of testing and a description of the change.