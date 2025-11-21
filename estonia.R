library(tidyverse)
library(nanoparquet)

estonia <- read_parquet("~/Desktop/R/work/estonia.parquet")
finance <- read_parquet("~/Desktop/R/work/finance.parquet")

glimpse(estonia)

estonia %>% count(kmkr_nr) %>% view

year_2019 <- read_delim("~/Desktop/R/work/4.2019_aruannete_elemendid_kuni_31102025.csv", col_types = c("cccc"))
year_2020 <- read_delim("~/Desktop/R/work/4.2020_aruannete_elemendid_kuni_31102025.csv", col_types = c("cccc"))
year_2021 <- read_delim("~/Desktop/R/work/4.2021_aruannete_elemendid_kuni_.csv", col_types = c("cccc"))
year_2022 <- read_delim("~/Desktop/R/work/4.2022_aruannete_elemendid_kuni_31102025.csv", col_types = c("cccc"))
year_2023 <- read_delim("~/Desktop/R/work/4.2023_aruannete_elemendid_kuni_31102025.csv", col_types = c("cccc"))
year_2024 <- read_delim("~/Desktop/R/work/4.2024_aruannete_elemendid_kuni_31102025.csv", col_types = c("cccc"))

finance <- bind_rows(year_2019, year_2020, year_2021, year_2022, year_2023, year_2024)

finance <- finance %>% 
  mutate(elemendi_nimetus = if_else(is.na(elemendi_nimetus), elemendi_label, elemendi_nimetus)) %>%
  mutate(elemendi_nimetus = fct_recode(elemendi_nimetus,
    "Revenue" = "Müügitulu",
    "RevenueConsolidated" = "Müügitulu Konsolideeritud",
    "Assets" = "Varad",
    "AssetsConsolidated" = "Varad Konsolideeritud",
    "AverageNumberOfEmployeesInFullTimeEquivalentUnits" = "Töötajate keskmine arv taandatuna täistööajale",
    "AverageNumberOfEmployeesInFullTimeEquivalentUnitsConsolidated" = "Töötajate keskmine arv taandatuna täistööajale Konsolideeritud")) %>%
  select(-tabel, -elemendi_label) %>%
  summarise(vaartus = sum(vaartus, na.rm = T), .by = c(report_id, elemendi_nimetus)) %>%
  pivot_wider(names_from = elemendi_nimetus, values_from = vaartus)

#------------------------------------------------------------------
general <- read_delim("~/Desktop/R/work/1.aruannete_yldandmed_kuni_31102025.csv", 
                      #col_select = c(1, 3, 4:6), 
                      col_types = c("ccc")) %>% 
  mutate(report_id = if_else(!is.na(täidetud_aruuanne_report_id), täidetud_aruuanne_report_id, report_id)) %>% 
  select(c(1, 3, 4:6))

emtak <- read_delim("~/Desktop/R/work/2.EMTAK_myygitulu_kuni_31102025.csv", col_types = c("ccdcc")) %>% 
  filter(põhitegevusala == "jah") %>% select(-põhitegevusala)
basic_data <- read_delim("~/Desktop/R/work/ettevotja_rekvisiidid__lihtandmed.csv", 
                         col_select = c(1:2, 5), col_types = c("ccc"))
#---------------------------------------------------------------------
joined <- left_join(general, emtak, by = c("report_id")) %>% 
  left_join(., basic_data, by = c("registrikood" = "ariregistri_kood"))

estonia <- left_join(finance, joined, by = c("report_id"))
#-----------------------------------------------------------------

general %>% map_dfr(~ sum(is.na(.)))

na_values <- estonia %>%
  map_dfr(~ sum(is.na(.))) %>%
  pivot_longer(everything()) %>%
  mutate(perc_na = round(value / nrow(estonia) * 100, 2)) %>%
  arrange(perc_na)

estonia_aruandeaast_na <- estonia %>%
  reframe(across(everything(), ~ round(sum(is.na(.) / n() * 100), 3)), .by = aruandeaast) %>%
  as_tibble() %>%
  pivot_longer(-aruandeaast, names_to = "index", values_to = "perc_na") %>%
  arrange(aruandeaast, perc_na) %>%
  select(index, aruandeaast, perc_na) %>%
  pivot_wider(names_from = aruandeaast, values_from = perc_na)

#============================================================
# write_parquet(estonia, "~/Desktop/R/work/estonia.parquet")
# write_csv(estonia, "~/Desktop/R/work/estonia.csv")
