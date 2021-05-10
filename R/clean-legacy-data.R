#' Clean legacy landings' catches table
#'
#' Legacy data include information from different versions of the project. This
#' implies that the same information can be associated to a different syntax and
#' to different column names. This function converts the information associated
#' with species catches into a standard format, the same one used for recent
#' landings.
#'
#' @param x A data frame containing raw legacy landings (SFF landings).
#'
#' @return A data frame containing species catches information in the same
#' syntax as recent landings.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' authentication_details <- readLines("location_of_json_file.json")
#'  # obtain the latest version of all files corresponding to timor-landings-v1
#'  legacy_data <-
#'  cloud_object_name(
#'  prefix = "timor-landings-v1",
#'  version = "latest",
#'  provider = "gcs",
#'  options = list(service_account_key = authentication_details,
#'                 bucket = "my-bucket"))
#'
#'  legacy_raw <-
#'  readr::read_csv(file = legacy_data, col_types = readr::cols(.default = readr::col_character()))
#'
#'  clean_catches(legacy_raw)
#' }
clean_catches <- function(x){

   # split catches data based on species prefixes
   prefix_list <-
      purrr::map(purrr::set_names(c("Spp.", "Species.","group_ob8uk86/Species/","group_ob8uk86/Species.","Species/")),
                 ~dplyr::select(x,tidyselect::starts_with(.x)))

   # standardize catches column names across splitted data
   for(i in 1:length(prefix_list)){
      colnames(prefix_list[[i]]) <-
         textclean::mgsub(names(prefix_list[[i]]),
                          pattern = c("Choose_Species","Species_001","presu_per_","nuumeru","other_name",
                                      "number_of_individuals","number_of_individuals_2","number_of_individuals_3","number_of_individuals_4",
                                      "number_of_individuals_5","number_of_individuals_6","number_of_individuals_7","number_of_individuals_8",
                                      "number_of_individuals_9","number_of_individuals_10","number_of_individuals_11",
                                      "NUMERU_IKAN_LIU_60_cm","IKAN_NARUK_LIU_60_cm_SUKAT_NO_",
                                      "take_a_picture_using_the_camera","food_or_sale_","group_ai0pp95",
                                      "Species","Spp","group_ob8uk86/Species.0.group_ob8uk86/Species","group_ob8uk86/Species"),
                          replacement = c("species","species","presu","numeru","other_species_name",
                                          "no_individuals_5_10","no_individuals_10_15","no_individuals_15_20","no_individuals_20_25",
                                          "no_individuals_25_30","no_individuals_30_35","no_individuals_35_40","no_individuals_40_45",
                                          "no_individuals_45_50","no_individuals_50_55","no_individuals_55_60",
                                          "fish_length_over60","no_individuals_over60",
                                          "photo","food_or_sale","no_fish_by_length_group",
                                          "species_group","species_group","species_group.0.species_group","species_group.0.species_group"),
                          fixed=TRUE)
   }

   # fix remaining syntax issues
   names(prefix_list$`Species/`) <- gsub(names(prefix_list$`Species/`),
                                         pattern="species_group",
                                         replacement="species_group.0.species_group")

   prefix_list$Spp. <- dplyr::coalesce(
      purrr::map(purrr::set_names(c("foodsale","food_or_sale")),~dplyr::select(prefix_list$Spp. ,tidyselect::contains(.x)))$foodsale %>%
         stats::setNames(gsub(names(.), pattern = "foodsale", replacement = "food_or_sale")),
      prefix_list$Spp. %>% dplyr::select(-tidyselect::ends_with("foodsale")))

   # coalesce new catches columns and drop unused info
   spe_table_cleaned <- dplyr::coalesce(!!!prefix_list) %>%
      dplyr::select(-c(tidyselect::contains("Fish_length",ignore.case = FALSE),
                       tidyselect::contains("_per_Kg_hira"),
                       tidyselect::contains("numeru_ikan"))) %>%
      dplyr::mutate(total_catch_value = as.character(rowSums(apply(dplyr::select(.,tidyselect::ends_with("presu")), 2,
                                                                   function(x) as.numeric(x)),na.rm=TRUE))) %>%
      dplyr::select(-tidyselect::ends_with("presu"))

   # remove groups' questions prefixes
   remove <- c("group_ob8uk86/","group_st04m70/","group_uw8jh69/")
   colnames(spe_table_cleaned) <- stringr::str_remove_all(names(spe_table_cleaned),
                                                          paste(remove, collapse = "|"))

   spe_table_cleaned
}

#' Coalesce vectors
#'
#' This function coalesces vectors from a selected data frame into a new single
#' vector. The vectors to coalesce are collected into a list (argument `to_coal`)
#' and the coalesced vector takes the name of the list' element in which it is
#' collected.
#'
#'
#' @param data Data frame from which vectors are selected.
#'
#' @param to_coal List of vectors to coalesce. Coalesced vectors assume the name
#' of the element of the list in which they are collected.
#'
#' @param return_dat Logical argument indicating whether to return the entire
#' data frame plus the new coalesced vectors or a new data frame constituted by
#' only the coalesced vectors.
#'
#' @return A data frame containing the coalesced vector(s)
#' @export
#'
#' @examples
#' \dontrun{
#' authentication_details <- readLines("location_of_json_file.json")
#'  # obtain the latest version of all files corresponding to timor-landings-v1
#'  legacy_data <-
#'  cloud_object_name(
#'  prefix = "timor-landings-v1",
#'  version = "latest",
#'  provider = "gcs",
#'  options = list(service_account_key = authentication_details,
#'                 bucket = "my-bucket"))
#'
#'  legacy_raw <-
#'  readr::read_csv(file = legacy_data, col_types =
#'  readr::cols(.default = readr::col_character()))
#'
#'  to_coal <- list("date" = legacy_raw %>% dplyr::select("Data","Date"),
#'                  "trip_group/gear_type" = legacy_raw %>%
#'                  dplyr::select(tidyselect::contains("gear")),
#'                  "trip_group/habitat_boat" = legacy_raw %>%
#'                  dplyr::select(tidyselect::contains("habitat")))
#'
#'  coalist(legacy_raw,to_coal,return_dat=FALSE)
#' }
#'
coalist <- function(data, to_coal, return_dat=FALSE){

   vecs <- NULL
   for(i in 1:length(to_coal)){
         vec <- dplyr::coalesce(!!!to_coal[[i]])
         vecs <- cbind(vecs,vec)
   }

   colnames(vecs) <- names(to_coal)

   if (return_dat==TRUE){

      cbind(data,vecs) %>%
         dplyr::select(-c(unlist(lapply(to_coal,colnames),use.names = FALSE))) %>%
         tidyr::as_tibble()

      } else {

         tidyr::as_tibble(vecs)
      }
   }


#' Clean legacy landings data
#'
#' Legacy landings data frame has a different structure and use a different
#' syntax from recent landings. This function converts legacy raw data into a
#' new and mergeable data frame with recent landings.
#'
#' @param x Data frame containing raw data from legacy landings (SSF landings).
#'
#' @return Data frame containing legacy landings data in the same syntax as
#' recent landings.
#' @export
#'
#' @examples
#' \dontrun{
#' authentication_details <- readLines("location_of_json_file.json")
#'  # obtain the latest version of all files corresponding to timor-landings-v1
#'  legacy_data <-
#'  cloud_object_name(
#'  prefix = "timor-landings-v1",
#'  version = "latest",
#'  provider = "gcs",
#'  options = list(service_account_key = authentication_details,
#'                 bucket = "my-bucket"))
#'
#'  legacy_raw <-
#'  readr::read_csv(file = legacy_data, col_types = readr::cols(.default = readr::col_character()))
#'
#'  clean_legacy_landings(legacy_raw)
#' }
clean_legacy_landings <- function(x){

   # store v1 and v2 common columns
   common_cols_table <- x %>% dplyr::select(
      c("__version__","_bamboo_dataset_id","_geolocation.0","_geolocation.1","_id",
        "_status","_submission_time","_submitted_by","_uuid","_validation_status.by_whom",
        "_validation_status.color","_validation_status.label","_validation_status.timestamp",
        "_validation_status.uid","_version_","_version__001","_xform_id_string","deviceid",
        "end","formhub/uuid","meta/instanceID","start","today"))

   # rename some v1 variables
   renamed_cols_table <- x %>% dplyr::select(c("Ita_kolecta_dadus_husi_activid","Site_name","No_boats")) %>%
      dplyr::rename("Ita_koleta_dadus_husi_atividad" = "Ita_kolecta_dadus_husi_activid",
             "landing_site_name"="Site_name",
             "no_boats"="No_boats")

   # list vectors to coalesce
   to_coal <- list("date" = x %>% dplyr::select("Data","Date"),
                   "trip_group/gear_type" = x %>% dplyr::select(tidyselect::contains("gear")),
                   "trip_group/habitat_boat" = x %>% dplyr::select(tidyselect::contains("habitat")),
                   "happiness_rating" = x %>% dplyr::select(tidyselect::contains("happiness")),
                   "trip_group/no_fishers/no_women_fisher" = x %>% dplyr::select(tidyselect::contains("women")),
                   "trip_group/no_fishers/no_child_fishers" = x %>% dplyr::select(tidyselect::contains("child")),
                   "trip_group/no_fishers/no_men_fishers" = x %>% dplyr::select(tidyselect::contains("mane_men")),
                   "trip_group/boat_owner_name" = x %>% dplyr::select(tidyselect::contains("fisher"),
                                                               -c("group_ob8uk86/fisher_name",
                                                                  "group_ob8uk86/peskador_naran_sa_fisher_name_",
                                                                  "peskador_naran_sa_fisher_name_")))

   # store coalesced vectors
   coalesced_table <- coalist(x,to_coal,return_dat=FALSE)
   # store catches
   catches_table <- clean_catches(x)
   # store attachments
   attachments_table <- x %>% dplyr::select(tidyselect::contains("attachment"))

   # bind the new vars
   dplyr::bind_cols(common_cols_table,
                    renamed_cols_table,
                    coalesced_table,
                    catches_table,
                    attachments_table)
}
