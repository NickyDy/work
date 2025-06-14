library(tidyverse)
library(readxl)
library(nanoparquet)

tourism <- read_excel("work/hr_tourist_reg_20250415.xlsx", sheet = 1) %>% distinct() %>% 
  mutate(operator_name = str_remove_all(operator_name, " d.o.o| d.d.| D.O.O.| j.d.o.o.| d.d|\\.|\\,.+|\\s[:lower:]+")) %>% 
  select(1:6, 14:17, everything())
rps <- read_parquet("work/rps.parquet") %>% 
  filter(rb == 0) %>% 
  distinct()

glimpse(tourism)

df_tourism <- inner_join(tourism, rps, by = c("operator_name" = "naziv_potpuni"))

tourism %>% count(operator_name, object, beds, sort = T) %>% view
rps %>% count(naziv_potpuni, sort = T) %>% view

df_tourism_na <- df_tourism %>%
  map_dfr(~ sum(is.na(.))) %>% 
  pivot_longer(everything()) %>%
  mutate(perc_na = round(value / nrow(df_tourism) * 100, 2)) %>% 
  arrange(perc_na)

rps_na <- rps %>%
  map_dfr(~ sum(is.na(.))) %>% 
  pivot_longer(everything()) %>%
  mutate(perc_na = round(value / nrow(rps) * 100, 2)) %>% 
  arrange(perc_na)
