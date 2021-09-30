#' Write a dataframe to Snowflake using a bulk copy method.
#'
#' @param dbCon (Mandatory). A DBIConnection object \code{\link[DBI]{dbConnect}}, as returned by dbConnect().
#' @param inputDF (Mandatory). A dataframe to be written to Snowflake.
#' @param outputTable (Mandatory). The name of the table object in Snowflake to write to. Can include database and schema qualifiers in front of the table name e.g. infodatabase.infoschema.mytablename.
#' @param append (Default = FALSE). A Boolean indicating whether the dataframe is appended to an existing table or a new table created in snowflake
#' @param overwrite (Default = FALSE). A Boolean indicating whether the dataframe should replace an existing table in snowflake
#' @param create_snowflake_format (Default = FALSE). A Boolean indicating to create a temporary CSV or JSON format for loading the file from the stage
#' @param create_snowflake_stage (Default = FALSE). A Boolean indicating to create a temporary load stage for the CSV or JSON file
#' @param stage_file_type (Default = "JSON"). Indicates whether to stage the file to be loaded into Snowflake as a CSV file or JSON file. A CSV file may be a little faster, however if your dataframe contains the Unit Separator character (ASCII 31, Hex 1F, which is the CSV delimiter), stage your file as a JSON file.
#' @param print_messages (Default = FALSE). Displays whether messages of the internal steps of this function are logged. Useful for debugging errors.
#'
#' @return The duration in seconds for the write operation to Snowflake.
#' @examples
#' my_write_time <- write_to_snowflake(dbCon, df_user_table, snowflake_target_table, append=TRUE)
#' my_write_time <- write_to_snowflake(dbCon, df_user_table, snowflake_target_table, append=TRUE, print_messages=TRUE)
#'
#' @description
#' This function assume a connection is already established to Snowflake. Supply the connection object, dataframe to write_to_snowflake, and the name of the target Snowflake table.
#'
#' The function will on a temporary basis stage the file you wish to load into Snowflake into temp location indicated by the tempdir() function. Please ensure your temp directory is sized appropriately for large datasets.
#' This file is then put to an internal Snowflake Stage and then copied into Snowflake. This generally performs a lot fast than direct ODBC write commands via R DBI / ODBC.
#'
#' You have options whether to persist the file as either a CSV for JSON file to load into Snowflake.
#'
#' You also can create the Snowflake Format to load the file on a temporary basis and also the Snowflake Stage. The names for the Snowflake format and stage are created dynamically and they are dropped once the load has completed.
#' There is an option to use an existing Format and Stage which has been created by your Snowflake Administrator in your database / schema. If you wish to use this approach because you may not have rights to create a Snowflake format and stages then you need to set the name for the format and stage via environment variables.
#'
#' Example: Setup Snowflake Environment (run by Snowflake Admin)
#'
#'   create file format FMT_ODBC_UNITSEP_CSV type = 'csv' field_delimiter = '0x1F' field_optionally_enclosed_by = '"' skip_header = 1;
#'
#'   create file format FMT_ODBC_JSON type = 'json' STRIP_OUTER_ARRAY = TRUE;
#'
#'   create stage STG_S3_ODBC_STG_CSV file_format = FMT_ODBC_UNITSEP_CSV;
#'
#'   create stage STG_S3_ODBC_STG_JSON file_format = FMT_ODBC_JSON;
#'
#' Example: Setup RStudio Environment
#'
#' Create the following environmental variables in RStudio or even better in your .Renviron / .Renviron.site or .RProfile / .Rprofile.site. The names should correspond with the setup by the Snowflake Administrator.
#' Note: You can optionally specify a schema which holds the Format and Stage e.g. COMMON.FMT_ODBC_UNITSEP_CSV
#'
#'   Sys.setenv(SNOWFLAKE_CSV_FORMAT_NAME = "FMT_ODBC_UNITSEP_CSV")
#'
#'   Sys.setenv(SNOWFLAKE_CSV_STAGE_NAME = "STG_S3_ODBC_STG_CSV")
#'
#'   Sys.setenv(SNOWFLAKE_JSON_FORMAT_NAME = "FMT_ODBC_JSON")
#'
#'   Sys.setenv(SNOWFLAKE_JSON_STAGE_NAME = "STG_S3_ODBC_STG_JSON")
#' @export
write_to_snowflake <- function(dbCon, inputDF, outputTable, append=FALSE, overwrite=FALSE, create_snowflake_format=FALSE, create_snowflake_stage=FALSE, stage_file_type="JSON", print_messages=FALSE) {

  # Quick Parameter check
  if (overwrite && append) {
    stop("Cannot use Overwrite and Append parameters together")
  }
  stage_file_type <- tolower(stage_file_type)
  if (!(stage_file_type == "csv") && !(stage_file_type == "json")) {
    stop("stage_file_type parameter must be either CSV or JSON")
  }
  if ((create_snowflake_stage && !create_snowflake_format) || (!create_snowflake_stage && create_snowflake_format)) {
    stop("Parameters create_snowflake_stage and create_snowflake_format must be both TRUE or both FALSE")
  }
  # Get the name of the dataframe
  inputDF_name              <- gsub("\\.", "", deparse(substitute(inputDF)) )

  # Set an output file name based on the input Dataframe name
  output_filename           <- paste0(tempdir(),"/",inputDF_name,".",stage_file_type)

  # Remove any previous instances of the file
  suppressMessages(unlink(output_filename))

  # Write JSON or a CSV file with a unit separator to help avoid collusions with commas. Note this is written to a users tmpdir
  if (stage_file_type == "csv") {
    valSep <- 31  # ASCII 31 which is the unit separator which we will use as a separator for the CSV file fields
    mode(valSep) <- "raw"
    valSep <- rawToChar(valSep)
    output_file_write_time    <- system.time(utils::write.table(janitor::clean_names(inputDF,"all_caps"),output_filename, sep = valSep, row.names = FALSE))
  } else {
    output_file_write_time    <- system.time(jsonlite::write_json(janitor::clean_names(inputDF,"all_caps"),output_filename))
  }

  # Set Names for Snowflake Format Name, Snowflake Stage Name
  if (create_snowflake_format) {
    snowflake_format_name <- paste0(Sys.getenv("LOGNAME"),"_format_",inputDF_name)
  } else {
    if (stage_file_type == "csv") {
      snowflake_format_name <- suppressMessages(Sys.getenv("SNOWFLAKE_CSV_FORMAT_NAME"))
    } else {
      snowflake_format_name <- suppressMessages(Sys.getenv("SNOWFLAKE_JSON_FORMAT_NAME"))
    }
    if (snowflake_format_name == "") {
      stop("Please set a default Snowflake Stage file format name to an existing Snowflake format e.g. Sys.setenv(SNOWFLAKE_CSV_FORMAT_NAME = \"FMT_ODBC_UNITSEP_CSV\")")
    }
  }
  if (create_snowflake_stage) {
    snowflake_stage_name  <- paste0(Sys.getenv("LOGNAME"),"_stage_",inputDF_name)
  } else {
    if (stage_file_type == "csv") {
      snowflake_stage_name <- suppressMessages(Sys.getenv("SNOWFLAKE_CSV_STAGE_NAME"))
    } else {
      snowflake_stage_name <- suppressMessages(Sys.getenv("SNOWFLAKE_JSON_STAGE_NAME"))
    }
    if (snowflake_stage_name == "") {
      stop("Please set a default Snowflake Stage name to an existing Snowflake Stage e.g. Sys.setenv(SNOWFLAKE_CSV_STAGE_NAME = \"STG_S3_ODBC_STG_CSV\")")
    }
  }

  # OPTIONAL : Create file format for loading the dataframe persisted file
  if (create_snowflake_format) {
    if (stage_file_type == "csv") {
      databaseQuery <- paste0("create file format ",snowflake_format_name," type = 'csv' field_delimiter = '0x1F' field_optionally_enclosed_by = '\"' skip_header = 1; ")
    } else {
      databaseQuery <- paste0("create file format ",snowflake_format_name," type = 'json' STRIP_OUTER_ARRAY = TRUE; ")
    }
    if(print_messages) {
      print(paste0("Format Command = ",databaseQuery))
    }
    snowflake_format_time <- system.time(v_format_create <- DBI::dbGetQuery(dbCon, databaseQuery))
  } else {
    snowflake_format_time <- system.time("1=1")
  }

  # OPTIONAL : Create a Snowflake Stage
  if (create_snowflake_stage) {

    databaseQuery <- paste0("create stage ",snowflake_stage_name," file_format = ",snowflake_format_name)
    if(print_messages) {
      print(paste0("Stage Command = ",databaseQuery))
    }
    snowflake_stage_time <- system.time(v_stage_create <- DBI::dbGetQuery(dbCon, databaseQuery))
  } else {
    snowflake_stage_time <- system.time("1=1")
  }

  # OPTIONAL : Create an empty table if append=FALSE
  if (append) {
    snowflake_table_create_time <- system.time("1=1")
  } else {
    # Prepare Table name for Create Table Command
    table_name_split <- unlist(strsplit(outputTable, "\\."))
    number_of_elements <- length(table_name_split)

    if (number_of_elements == 1) {
      table_id <- DBI::Id(table = table_name_split[1])
    } else if ( number_of_elements == 2) {
      table_id <- DBI::Id(schema = table_name_split[1],
                          table = table_name_split[2])
    } else if ( number_of_elements == 3) {
      table_id <- DBI::Id(database = table_name_split[1],
                          schema = table_name_split[2],
                          table = table_name_split[3])
    } else {
      stop(paste0("Invalid Table Name ",table_name_split))
    }

    tempDF <- head(janitor::clean_names(inputDF,"all_caps"),0)
    sql_create_stmt <- DBI::sqlCreateTable(dbCon, table_id, tempDF)
    if (overwrite) {
      sql_create_stmt <- gsub("CREATE TABLE", "CREATE OR REPLACE TABLE", sql_create_stmt)
    }
    snowflake_table_create_time <- system.time(v_table_create <- DBI::dbGetQuery(dbCon, sql_create_stmt))

  }


  # Put / Upload the Dataframe file to the stage
  databaseQuery <- paste0("put 'file://",Sys.getenv("R_SESSION_TMPDIR"),"/",inputDF_name,".",stage_file_type,"' @",snowflake_stage_name)

  if(print_messages) {
    print(paste0("Put Command = ",databaseQuery))
  }
  snowflake_put_time <- system.time(v_put_file <- DBI::dbGetQuery(dbCon, databaseQuery))

  # OPTIONAL : In print mode is enabled, list files available in the Stage - Useful for debugging
  if(print_messages) {
    databaseQuery <- paste0("list @",snowflake_stage_name)
    print(paste0("List Command = ",databaseQuery))
    snowflake_list_time <- system.time(v_list_file <- DBI::dbGetQuery(dbCon, databaseQuery))
    print(v_list_file)
  }

  # Copy from the Stage to Snowflake destination
  if (stage_file_type == "csv") {
    databaseQuery <- paste0("COPY INTO ",outputTable," FROM @",snowflake_stage_name," FILES = ( '",inputDF_name,".",stage_file_type,".gz' ) ")
  } else {
    databaseQuery <- paste0("COPY INTO ",outputTable," FROM @",snowflake_stage_name," FILES = ( '",inputDF_name,".",stage_file_type,".gz' ) MATCH_BY_COLUMN_NAME = CASE_SENSITIVE")
  }

  if(print_messages) {
    print(paste0("Copy Command = ",databaseQuery))
  }
  snowflake_copy_time <- system.time(v_copy_file <- DBI::dbGetQuery(dbCon, databaseQuery))

  # OPTIONAL : Drop the temporary Stage else remove the uploaded file
  if (create_snowflake_stage) {
    databaseQuery <- paste0("drop stage ",snowflake_stage_name)
    if(print_messages) {
      print(paste0("Drop Stage Command = ",databaseQuery))
    }
    snowflake_drop_stage <- system.time(v_stage_create <- DBI::dbGetQuery(dbCon, databaseQuery))
  } else {
    databaseQuery <- paste0("remove @",snowflake_stage_name,"/",inputDF_name,".",stage_file_type,".gz")
    if(print_messages) {
      print(paste0("Remove uploaded file Command = ",databaseQuery))
    }
    snowflake_drop_stage <- system.time(v_stage_create <- DBI::dbGetQuery(dbCon, databaseQuery))
  }

  # OPTIONAL : Drop the temporary Snowflake file Format
  if (create_snowflake_format) {
    databaseQuery <- paste0("drop file format ",snowflake_format_name)
    if(print_messages) {
      print(paste0("Drop file format Command = ",databaseQuery))
    }
    snowflake_drop_format <- system.time(v_format_create <- DBI::dbGetQuery(dbCon, databaseQuery))
  } else {
    snowflake_drop_format <- system.time("1=1")
  }

  # Remove the temporary output file which was created
  unlink(output_filename)

  snowflake_put_copy_time <- output_file_write_time[3] +
    snowflake_format_time[3] +
    snowflake_stage_time[3] +
    snowflake_table_create_time[3] +
    snowflake_put_time[3] +
    snowflake_copy_time[3] +
    snowflake_drop_format[3] +
    snowflake_drop_stage[3]

  return(snowflake_put_copy_time)
}
