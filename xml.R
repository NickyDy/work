library(tidyverse)
library(nanoparquet)
library(xml2)
options(scipen = 100)

#xml <- read_xml('work/xml/2014BG05M2OP001 Наука и образование за интелигентен растеж.xml')
programs <- read_parquet("work/programs.parquet")
contracts <- read_parquet("work/contracts.parquet")
entities <- read_parquet("work/entities.parquet")

write_parquet(entities, "work/entities.parquet")

glimpse(entities)
entities %>% count(programa, entity_uin, sort = T) %>% view

library(fs)
files <- dir_ls("work/xml", glob = "*.xml")
paths <- list.files("work/xml", pattern = "[.]xml$")
#PROJECTS--------------------------
read_projects <- function(files) ({
  
  xml <- read_xml(files)
  
  projects <- xml %>% 
    xml_find_all(".//Projects/Project")
  
tibble(
  project_id = xml_find_first(projects, ".//Id") %>% xml_text(),
  source_of_funding = xml_find_first(projects, ".//SourceofFunding") %>% xml_text(),
  initial_date_p = xml_find_first(projects, ".//InitialDate") %>% xml_text(),
  end_date_p = xml_find_first(projects, ".//EndDate") %>% xml_text(),
  #date_of_decision_for_funding = xml_find_first(projects, ".//DateofDecisionforFunding") %>% xml_text(),
  #project_beneficiary = xml_find_first(projects, ".//ProjectBeneficiary") %>% xml_text(),
  # name = xml_find_first(projects, ".//Name") %>% xml_text(),
  # total_value = xml_find_first(projects, ".//TotalValue") %>% xml_text(),
  #beneficiary_funding = xml_find_first(projects, ".//BeneficiaryFunding") %>% xml_text(),
  #actually_paid_amounts = xml_find_first(projects, ".//ActuallyPaidAmounts/ActuallyPaidAmount/Value") %>% xml_text(),
  # duration_months = xml_find_first(projects, ".//DurationInMonths") %>% xml_text(),
  descripton = xml_find_first(projects, ".//Description") %>% xml_text(),
  # status = xml_find_first(projects, ".//Status") %>% xml_text()
)

})

projects <- map(files, read_projects) %>% bind_rows() %>% distinct()

projects <- projects %>%
  mutate(initial_date_p = ymd_hms(initial_date_p),
         end_date_p = ymd_hms(end_date_p)) %>% distinct()

projects <- projects_df %>% 
  left_join(., projects, by = c("nomer_na_proektno_predlozenie" = "project_id"))

#CONTRACTS
read_contracts <- function(files) ({
  
  xml <- read_xml(files)
  
  contracts <- xml %>% 
    xml_find_all(".//Contracts/Contract")
  
  tibble(
    entity_id = xml_find_first(contracts, ".//EntityId") %>% xml_text(),
    contract_id = xml_find_first(contracts, ".//ContractId") %>% xml_text(),
    signature_date = xml_find_first(contracts, ".//SignatureDate") %>% xml_text(),
    initial_date_c = xml_find_first(contracts, ".//InitialDate") %>% xml_text(),
    end_date_c = xml_find_first(contracts, ".//EndDate") %>% xml_text(),
    #entity_description = xml_find_first(contracts, ".//Description") %>% xml_text(),
    amount = xml_find_first(contracts, ".//Amount") %>% xml_text())
  
})

contracts <- map(files, read_contracts) %>% bind_rows() %>% distinct()

contracts <- contracts %>% 
  # mutate(project_id = str_extract(contract_id, "^.+(?=\\()")) %>%
  # mutate(contract_id = str_extract(contract_id, "(?<=\\().+$")) %>%
  # mutate(contract_id = str_remove_all(contract_id, "\\(|\\)")) %>% 
  # mutate(project_id = str_remove(project_id, "\\(|\\(.+")) %>% 
  # select(project_id, contract_id, everything()) %>% 
  mutate(across(signature_date:end_date_c, ymd_hms)) %>% 
  mutate(amount = as.numeric(amount))

#ENTITIES
read_entities <- function(files) ({
  
xml <- read_xml(files)
  
entities <- xml %>% 
  xml_find_all(".//Entities/Entity")

tibble(
  entity_id = xml_find_first(entities, ".//EntityId") %>% xml_text(),
  entity_uin = xml_find_first(entities, ".//EntityUin") %>% xml_text(),
  entity_name = xml_find_first(entities, ".//EntityName") %>% xml_text(), 
  entity_zip_code = xml_find_first(entities, ".//EntityZipCode") %>% xml_text(),
  entity_city = xml_find_first(entities, ".//EntityCity") %>% xml_text(),
  entity_mun = xml_find_first(entities, ".//EntityMunicipality") %>% xml_text(), 
  entity_district = xml_find_first(entities, ".//EntityDistrict") %>% xml_text())

})

entities <- map(files, read_entities) %>% 
  set_names(basename) %>% 
  list_rbind(names_to = "programa") %>% 
  distinct()

entities <- entities %>% 
  mutate(programa = str_remove(programa, ".xml$"))
#-------------------------------------------------------------------
entities %>% map_dfr(~sum(is.na(.))) %>% view

glimpse(entities)

contracts %>% distinct()

write_csv(entities, "work/entities.csv")
write_parquet(entities, "work/entities.parquet")
