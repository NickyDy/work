library(tidyverse)
library(xml2)

xml <- read_xml('work/xml/2021BG-RRP Национален план за възстановяване и устойчивост.xml')

#PROJECTS
project_id <- xml_text(xml_find_all(xml, ".//Project/Id"))
# source_funding <- xml_text(xml_find_all(xml, ".//Project/SourceofFunding"))
initial_date_p <- xml_text(xml_find_all(xml, ".//Project/InitialDate"))
end_date_p <- xml_text(xml_find_all(xml, ".//Project/EndDate"))
# decision_date <- xml_text(xml_find_all(xml, ".//Project/DateofDecisionforFunding"))
# project_beneficiary <- xml_text(xml_find_all(xml, ".//Project/ProjectBeneficiary"))
# project_name <- xml_text(xml_find_all(xml, ".//Project/Name"))
# total_value <- xml_text(xml_find_all(xml, ".//Project/TotalValue"))
# beneficiary_funding <- xml_text(xml_find_all(xml, ".//Project/BeneficiaryFunding"))
# actually_paid_amounts <- xml_text(xml_find_all(xml, ".//Project/ActuallyPaidAmounts"))
# duration_months <- xml_text(xml_find_all(xml, ".//Project/DurationInMonths"))
# project_description <- xml_text(xml_find_all(xml, ".//Project/Description"))
# status <- xml_text(xml_find_all(xml, ".//Project/Status"))

projects_xml <- tibble(project_id, initial_date_p, end_date_p) %>%
  mutate(initial_date_p = ymd_hms(initial_date_p),
         end_date_p = ymd_hms(end_date_p)) %>% 
  distinct()

df <- projects_df %>% filter(programa == '2021BG14MFPR001 Програма за морско дело, рибарство и аквакултури 2021-2027 г.') %>% 
  left_join(., projects_xml, by = c("nomer_na_proektno_predlozenie" = "project_id"))

glimpse(df)

write_csv(df, 'work/2021BG14MFPR001 Програма за морско дело, рибарство и аквакултури 2021-2027.csv')

#CONTRACTS
entity_id <- xml_text(xml_find_all(xml, ".//Contract/EntityId"))
contract_id <- xml_text(xml_find_all(xml, ".//Contract/ContractId"))
signature_date <- xml_text(xml_find_all(xml, ".//Contract/SignatureDate"))
initial_date_c <- xml_text(xml_find_all(xml, ".//Contract/InitialDate"))
end_date_c <- xml_text(xml_find_all(xml, ".//Contract/EndDate"))
entity_description <- xml_text(xml_find_all(xml, ".//Contract/Description"))
amount <- xml_text(xml_find_all(xml, ".//Contract/Amount"))

#ENTITIES
# entity_entity_id <- xml_text(xml_find_all(xml, ".//Entity/EntityId"))
# entity_uin <- xml_text(xml_find_all(xml, ".//Entity/EntityUin"))
# entity_name <- xml_text(xml_find_all(xml, ".//Entity/EntityName"))
# entity_zip_code <- xml_text(xml_find_all(xml, ".//Entity/EntityZipCode"))
# entity_city <- xml_text(xml_find_all(xml, ".//Entity/EntityCity"))
# entity_mun <- xml_text(xml_find_all(xml, ".//Entity/EntityMunicipality"))
# entity_district <- xml_text(xml_find_all(xml, ".//Entity/EntityDistrict"))

contractors_xml <- tibble(contract_id, signature_date, initial_date_c, end_date_c, 
                          amount) %>%
  mutate(initial_date_c = ymd_hms(initial_date_c),
         end_date_c = ymd_hms(end_date_c),
         amount = as.numeric(amount)) %>% 
  mutate(project_id = str_sub(contract_id, start = 1L, end = 22L),
         contract_id = str_sub(contract_id, start = 23L, end = 100)) %>%
  select(project_id, contract_id, everything()) %>% distinct()

glimpse(xmls)

library(fs)
files <- dir_ls("work/xml", glob = "*.xml")

read_entity <- function(files) ({
  
  xml <- read_xml(files)
  
  entity_id <- xml_text(xml_find_all(xml, ".//Contract/EntityId"))
  contract_id <- xml_text(xml_find_all(xml, ".//Contract/ContractId"))
  signature_date <- xml_text(xml_find_all(xml, ".//Contract/SignatureDate"))
  initial_date_c <- xml_text(xml_find_all(xml, ".//Contract/InitialDate"))
  end_date_c <- xml_text(xml_find_all(xml, ".//Contract/EndDate"))
  #entity_description <- xml_text(xml_find_all(xml, ".//Contract/Description"))
  amount <- xml_text(xml_find_all(xml, ".//Contract/Amount"))
  
contractors_xml <- tibble(contract_id, signature_date,
                          #entity_description,
                          initial_date_c, end_date_c, amount) %>%
    mutate(initial_date_c = ymd_hms(initial_date_c),
           end_date_c = ymd_hms(end_date_c),
           signature_date = ymd_hms(signature_date),
           amount = as.numeric(amount)) %>% 
    mutate(project_id = str_sub(contract_id, start = 1L, end = 22L),
           contract_id = str_sub(contract_id, start = 23L, end = 100)) %>%
  select(project_id, contract_id, everything()) %>% distinct()

})

contracts <- map(files, read_entity) %>% bind_rows()

contracts %>% map_dfr(~sum(is.na(.))) %>% view

contracts %>% count(project_id, contract_id, sort = T) %>% view

contracts %>% filter(amount == 0)

glimpse(contracts)

