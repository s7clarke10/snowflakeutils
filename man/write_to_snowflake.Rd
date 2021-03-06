% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/write_to_snowflake.R
\name{write_to_snowflake}
\alias{write_to_snowflake}
\title{Write a dataframe to Snowflake using a bulk copy method.}
\usage{
write_to_snowflake(
  dbCon,
  inputDF,
  outputTable,
  append = FALSE,
  overwrite = FALSE,
  create_snowflake_format = FALSE,
  create_snowflake_stage = FALSE,
  stage_file_type = "JSON",
  print_messages = FALSE
)
}
\arguments{
\item{dbCon}{(Mandatory). A DBIConnection object \code{\link[DBI]{dbConnect}}, as returned by dbConnect().}

\item{inputDF}{(Mandatory). A dataframe to be written to Snowflake.}

\item{outputTable}{(Mandatory). The name of the table object in Snowflake to write to. Can include database and schema qualifiers in front of the table name e.g. infodatabase.infoschema.mytablename.}

\item{append}{(Default = FALSE). A Boolean indicating whether the dataframe is appended to an existing table or a new table created in snowflake}

\item{overwrite}{(Default = FALSE). A Boolean indicating whether the dataframe should replace an existing table in snowflake}

\item{create_snowflake_format}{(Default = FALSE). A Boolean indicating to create a temporary CSV or JSON format for loading the file from the stage}

\item{create_snowflake_stage}{(Default = FALSE). A Boolean indicating to create a temporary load stage for the CSV or JSON file}

\item{stage_file_type}{(Default = "JSON"). Indicates whether to stage the file to be loaded into Snowflake as a CSV file or JSON file. A CSV file may be a little faster, however if your dataframe contains the Unit Separator character (ASCII 31, Hex 1F, which is the CSV delimiter), stage your file as a JSON file.}

\item{print_messages}{(Default = FALSE). Displays whether messages of the internal steps of this function are logged. Useful for debugging errors.}
}
\value{
The duration in seconds for the write operation to Snowflake.
}
\description{
This function assume a connection is already established to Snowflake. Supply the connection object, dataframe to write to Snowflake, and the name of the target Snowflake table.

The function will on a temporary basis stage the file you wish to load into Snowflake into temp location indicated by the tempdir() function. Please ensure your temp directory is sized appropriately for large datasets.
This file is then put to an internal Snowflake Stage and then copied into Snowflake. This generally performs a lot fast than direct ODBC write commands via R DBI / ODBC.

You have options whether to persist the file as either a CSV for JSON file to load into Snowflake.

You also can create the Snowflake Format to load the file on a temporary basis and also the Snowflake Stage. The names for the Snowflake format and stage are created dynamically and they are dropped once the load has completed.
There is an option to use an existing Format and Stage which has been created by your Snowflake Administrator in your database / schema. If you wish to use this approach because you may not have rights to create a Snowflake format and stages then you need to set the name for the format and stage via environment variables.

Example: Setup Snowflake Environment (run by Snowflake Admin)

create file format FMT_ODBC_UNITSEP_CSV type = 'csv' field_delimiter = '0x1F' field_optionally_enclosed_by = '"' skip_header = 1;

create file format FMT_ODBC_JSON type = 'json' STRIP_OUTER_ARRAY = TRUE;

create stage STG_S3_ODBC_STG_CSV file_format = FMT_ODBC_UNITSEP_CSV;

create stage STG_S3_ODBC_STG_JSON file_format = FMT_ODBC_JSON;

Example: Setup RStudio Environment

Create the following environmental variables in RStudio or even better in your .Renviron / .Renviron.site or .RProfile / .Rprofile.site. The names should correspond with the setup by the Snowflake Administrator.
Note: You can optionally specify a schema which holds the Format and Stage e.g. COMMON.FMT_ODBC_UNITSEP_CSV

Sys.setenv(SNOWFLAKE_CSV_FORMAT_NAME = "FMT_ODBC_UNITSEP_CSV")

Sys.setenv(SNOWFLAKE_CSV_STAGE_NAME = "STG_S3_ODBC_STG_CSV")

Sys.setenv(SNOWFLAKE_JSON_FORMAT_NAME = "FMT_ODBC_JSON")

Sys.setenv(SNOWFLAKE_JSON_STAGE_NAME = "STG_S3_ODBC_STG_JSON")
}
\examples{
my_write_time <- write_to_snowflake(dbCon, df_user_table, snowflake_target_table, append=TRUE)
my_write_time <- write_to_snowflake(dbCon, df_user_table, snowflake_target_table, append=TRUE, print_messages=TRUE)

}
