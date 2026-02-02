library(tidyverse)
library(nanoparquet)
library(fs)
library(readxl)
options(scipen = 100)

romania <- read_parquet("work/parquet/romania.parquet")

glimpse(romania)

agents <- read_excel("work/20250821-ro-tour-agents_v1.xlsx", skip = 6) %>% janitor::clean_names()
hotels <- read_excel("work/20250821-ro-tour-hotels_v1.xlsx", skip = 6) %>% janitor::clean_names()
food_drink <- read_excel("work/20250821-ro-tour-food-drink_v1.xlsx", skip = 6) %>% janitor::clean_names()

food_drink %>% count(no_of_seats, sort = T) %>% view

agents_count <- agents %>% count(unique_registration_code, sort = T) %>% drop_na()
hotels_count <- hotels %>% count(unique_registration_code, sort = T) %>% drop_na() %>% 
  filter(unique_registration_code != "#N/A")
food_drink_count <- food_drink %>% count(unique_registration_code, sort = T) %>% drop_na() %>% 
  filter(unique_registration_code != "#N/A")

romania_join <- romania %>% 
  filter(year == 2021) %>% select(year, cui, employees, nace2)

agents_joined <- agents_count %>% left_join(romania_join, by = c("unique_registration_code" = "cui"))
hotels_joined <- hotels_count %>% left_join(romania_join, by = c("unique_registration_code" = "cui"))
food_drink_joined <- food_drink_count %>% left_join(romania_join, by = c("unique_registration_code" = "cui"))

romania_na <- romania %>%
  map_dfr(~ sum(is.na(.))) %>%
  pivot_longer(everything()) %>%
  mutate(perc_na = round(value / nrow(romania) * 100, 2)) %>%
  arrange(perc_na)
#----------------------
romania_year_na <- romania %>%
  reframe(across(everything(), ~ round(sum(is.na(.) / n() * 100), 3)), .by = year) %>%
  as_tibble() %>%
  pivot_longer(-year, names_to = "index", values_to = "perc_na") %>%
  arrange(year, perc_na) %>%
  select(index, year, perc_na) %>%
  pivot_wider(names_from = year, values_from = perc_na)

romania %>%
  count(cui) %>%
  view()

write_csv(food_drink_joined, "work/food_drink_joined_2021.csv")
write_parquet(romania, "work/romania.parquet")

plot_cui("10016179")

plot_cui <- function(number) {
  title <- romania %>%
    filter(cui == number)

  romania %>%
    filter(cui == number) %>%
    pivot_longer(assets_b:employees) %>%
    drop_na() %>%
    mutate(col = value > 0, name = fct_inorder(name)) %>%
    ggplot(aes(year, value, fill = col, group = name)) +
    geom_point(show.legend = F) +
    geom_line(linetype = 2, linewidth = 0.2) +
    labs(title = paste0("CUI: ", unique(title$cui))) +
    facet_wrap(vars(name), scales = "free_y", ncol = 3)
}

read_csv_cc <- function(file) {
  read_csv(file, col_types = "cc")
}
# WEB_BL--------------------------------------
files <- dir_ls("work/rom_txt", regexp = "bl")
web_bl <- map(files, read_csv_cc) %>%
  set_names(basename) %>%
  list_rbind(names_to = "filename") %>%
  mutate(year = str_extract(filename, "\\d+"), source = "BL") %>%
  select(year, source, cui:numar_mediu_de_salariati) %>%
  distinct()

web_bl_na <- web_bl %>%
  map_dfr(~ sum(is.na(.))) %>%
  pivot_longer(everything()) %>%
  mutate(perc_na = round(value / nrow(web_bl) * 100, 2)) %>%
  arrange(perc_na)

web_bl_year_na <- web_bl %>%
  reframe(across(everything(), ~ round(sum(is.na(.) / n() * 100), 3)), .by = year) %>%
  as_tibble() %>%
  pivot_longer(-year, names_to = "index", values_to = "perc_na") %>%
  arrange(year, perc_na) %>%
  select(index, year, perc_na) %>%
  pivot_wider(names_from = year, values_from = perc_na)

write_csv(web_bl, "work/web_bl.csv")
write_parquet(web_bl, "work/web_bl.parquet")

# WEB_IR--------------------------------------
files <- dir_ls("work/rom_txt", regexp = "ir")
web_ir <- map(files, read_csv_cc) %>%
  set_names(basename) %>%
  list_rbind(names_to = "filename") %>%
  mutate(year = str_extract(filename, "\\d+"), source = "IR") %>%
  select(year, source, cui:patrimoniul_regiei) %>%
  distinct()

web_ir_na <- web_ir %>%
  map_dfr(~ sum(is.na(.))) %>%
  pivot_longer(everything()) %>%
  mutate(perc_na = round(value / nrow(web_ir) * 100, 2)) %>%
  arrange(perc_na)

web_ir_year_na <- web_ir %>%
  reframe(across(everything(), ~ round(sum(is.na(.) / n() * 100), 3)), .by = year) %>%
  as_tibble() %>%
  pivot_longer(-year, names_to = "index", values_to = "perc_na") %>%
  arrange(year, perc_na) %>%
  select(index, year, perc_na) %>%
  pivot_wider(names_from = year, values_from = perc_na)

write_csv(web_ir, "work/web_ir.csv")
write_parquet(web_ir, "work/web_ir.parquet")

# WEB_UU--------------------------------------
files <- dir_ls("work/rom_txt", regexp = "uu")
web_uu <- map(files, read_csv_cc) %>%
  set_names(basename) %>%
  list_rbind(names_to = "filename") %>%
  mutate(year = str_extract(filename, "\\d+"), , source = "UU") %>%
  select(year, source, cui:patrimoniul_regiei) %>%
  distinct()

web_uu_na <- web_uu %>%
  map_dfr(~ sum(is.na(.))) %>%
  pivot_longer(everything()) %>%
  mutate(perc_na = round(value / nrow(web_uu) * 100, 2)) %>%
  arrange(perc_na)

web_uu_year_na <- web_uu %>%
  reframe(across(everything(), ~ round(sum(is.na(.) / n() * 100), 3)), .by = year) %>%
  as_tibble() %>%
  pivot_longer(-year, names_to = "index", values_to = "perc_na") %>%
  arrange(year, perc_na) %>%
  select(index, year, perc_na) %>%
  pivot_wider(names_from = year, values_from = perc_na)

write_csv(web_uu, "work/web_uu.csv")
write_parquet(web_uu, "work/web_uu.parquet")
#---------------------------------------------------
# CSVs
# read_csv_rn <- function(file){
#   read_csv(file, col_names = F)
# }
#
# files <- dir_ls("work/rom_csvs", regexp = "bl")
# csv_bl <- map(files, read_csv_rn) %>%
#   set_names(basename) %>%
#   list_rbind(names_to = "filename") %>%
#   mutate(year = str_extract(filename, "\\d+")) %>%
#   select(year, X1)
#
# csv_ir %>% count(year)
#
# files <- dir_ls("work/rom_csvs", regexp = "ir")
# csv_ir <- map(files, read_csv_rn) %>%
#   set_names(basename) %>%
#   list_rbind(names_to = "filename") %>%
#   mutate(year = str_extract(filename, "\\d+")) %>%
#   select(year, X1)
#
# files <- dir_ls("work/rom_csvs", regexp = "UUuu")
# csv_uu <- map(files, read_csv) %>%
#   set_names(basename) %>%
#   list_rbind(names_to = "filename") %>%
#   mutate(year = str_extract(filename, "\\d+")) %>%
#   select(year, CUI:I20) %>%
#   distinct()
#------------------------------
library(curl)
multi_download(links_bl)
curl_download(
  "https://data.gov.ro/dataset/d3caacb6-2c08-445e-94e6-8d36d00ab250/resource/f89140dc-20dd-494f-912a-d1a482188885/download/web_bl_bs_sl_an2024.txt",
  "work/bl_2024.txt"
)
#
# links_bl <- c("https://data.gov.ro/dataset/d3caacb6-2c08-445e-94e6-8d36d00ab250/resource/f89140dc-20dd-494f-912a-d1a482188885/download/web_bl_bs_sl_an2024.txt",
#               "https://data.gov.ro/dataset/7861a98f-4d5c-4faa-90d4-8e934ebd1782/resource/5ed47b6f-f8a2-4ca8-a272-692aff4fe9e4/download/web_bl_bs_sl_an2023.txt",
#               "https://data.gov.ro/dataset/aa2567a4-e7d7-4e6e-ab19-d08d39f99996/resource/b35fab04-f101-42d7-a765-8f41728b373a/download/web_bl_bs_sl_an2022.txt",
#               "https://data.gov.ro/dataset/f8353c0e-fee9-4aa3-b26d-be0e96c328a7/resource/d0a42232-5aa0-425a-b264-67184e8ded5f/download/web_bl_bs_sl_an2021.txt",
#               "https://data.gov.ro/dataset/e977e5b9-0a1f-46ac-8cb8-f856a011a8ed/resource/00618bb2-b8b7-4861-95f0-184871aab230/download/web_bl_bs_sl_an2020.txt",
#               "https://data.gov.ro/dataset/0be1e2aa-8399-4cfc-beab-bf516fcc8f16/resource/983c2a11-360c-4e42-a441-2ab13310592d/download/web_bl_bs_sl_an2019.txt",
#               "https://data.gov.ro/dataset/a9c6dd10-dee2-46e9-aa3f-58dea0e50396/resource/aa694624-d14a-4cc9-9a09-9eaa7c97a0ab/download/webblbsslan2018.txt",
#               "https://data.gov.ro/dataset/f3c94174-4991-4d25-b183-663370908de3/resource/b00a63a0-4916-459a-a5d8-34cc68d34e86/download/webblbsslan2017.txt",
#               "https://data.gov.ro/dataset/e6274edc-fe36-4a79-ba73-c05711b70d80/resource/a71a4687-84eb-4d5d-a315-3dc38f4ea97f/download/webblbsslan2016.txt",
#               "https://data.gov.ro/dataset/6a021146-6a0d-4262-8b0e-2b43aa79bca7/resource/098a544a-2ced-418c-ac93-f72b4a8c1c5a/download/webblbsslan2015.txt",
#               "https://data.gov.ro/dataset/77ea3bbe-a533-4db0-8647-d380a75a39b5/resource/5563f94a-8ca3-4739-8036-75ed9bb386aa/download/webblbsslan2014.txt"
#               )

# WEB_BL_BS_SL_AN2024 <- read_csv("https://data.gov.ro/dataset/d3caacb6-2c08-445e-94e6-8d36d00ab250/resource/f89140dc-20dd-494f-912a-d1a482188885/download/web_bl_bs_sl_an2024.txt",
#                                 col_types = "cc")
# WEB_IR_2024 <- read_csv("https://data.gov.ro/dataset/d3caacb6-2c08-445e-94e6-8d36d00ab250/resource/a800aa00-e75e-402c-ae58-7746e224d1d7/download/web_ir_an2024.txt",
#                         col_types = "cc")
# WEB_UU_AN2024 <- read_csv("https://data.gov.ro/dataset/d3caacb6-2c08-445e-94e6-8d36d00ab250/resource/25098618-f6a5-4610-8c7f-c0bdb801635f/download/web_uu_an2024.txt",
#                           col_types = "cc")

# WEB_BL_BS_SL_AN2023 <- read_csv("https://data.gov.ro/dataset/7861a98f-4d5c-4faa-90d4-8e934ebd1782/resource/5ed47b6f-f8a2-4ca8-a272-692aff4fe9e4/download/web_bl_bs_sl_an2023.txt",
#                                 col_types = "cc")
# WEB_IR_2023 <- read_csv("https://data.gov.ro/dataset/7861a98f-4d5c-4faa-90d4-8e934ebd1782/resource/3e8b016f-5c4e-443e-b2b8-30adee22a6ea/download/web_ir_an2023.txt",
#                         col_types = "cc")
# WEB_UU_AN2023 <- read_csv("https://data.gov.ro/dataset/7861a98f-4d5c-4faa-90d4-8e934ebd1782/resource/e1caf82b-7f44-446f-8f53-91b43ec89654/download/web_uu_an2023.txt",
#                           col_types = "cc")

# WEB_BL_BS_SL_AN2022 <- read_csv("https://data.gov.ro/dataset/aa2567a4-e7d7-4e6e-ab19-d08d39f99996/resource/b35fab04-f101-42d7-a765-8f41728b373a/download/web_bl_bs_sl_an2022.txt",
#                                 col_types = "cc")
# WEB_IR_2022 <- read_csv("https://data.gov.ro/dataset/aa2567a4-e7d7-4e6e-ab19-d08d39f99996/resource/f1c5217b-2204-4961-8264-f07ab55e3f2d/download/web_ir_an2022.txt",
#                         col_types = "cc")
# WEB_UU_AN2022 <- read_csv("https://data.gov.ro/dataset/aa2567a4-e7d7-4e6e-ab19-d08d39f99996/resource/7f31544a-1f85-4700-8fe6-9cd19fcf8515/download/web_uu_an2022.txt",
#                           col_types = "cc")

# WEB_BL_BS_SL_AN2021 <- read_csv("https://data.gov.ro/dataset/f8353c0e-fee9-4aa3-b26d-be0e96c328a7/resource/d0a42232-5aa0-425a-b264-67184e8ded5f/download/web_bl_bs_sl_an2021.txt",
#                                 col_types = "cc")
# WEB_IR_2021 <- read_csv("https://data.gov.ro/dataset/f8353c0e-fee9-4aa3-b26d-be0e96c328a7/resource/15e36a41-ac3d-4494-a506-be122a2ace4d/download/web_ir_an2021.txt",
#                         col_types = "cc")
# WEB_UU_AN2021 <- read_csv("https://data.gov.ro/dataset/f8353c0e-fee9-4aa3-b26d-be0e96c328a7/resource/d9411926-c66d-4b27-a28c-7b590425efd3/download/web_uu_an2021.txt",
#                           col_types = "cc")

# WEB_BL_BS_SL_AN2020 <- read_csv("https://data.gov.ro/dataset/e977e5b9-0a1f-46ac-8cb8-f856a011a8ed/resource/00618bb2-b8b7-4861-95f0-184871aab230/download/web_bl_bs_sl_an2020.txt",
#                                 col_types = "cc")
# WEB_IR_2020 <- read_csv("https://data.gov.ro/dataset/f8353c0e-fee9-4aa3-b26d-be0e96c328a7/resource/789e329a-59c7-4557-a061-43068b514019/download/web_ir_an2020.txt",
#                         col_types = "cc")
# WEB_UU_AN2020 <- read_csv("https://data.gov.ro/dataset/e977e5b9-0a1f-46ac-8cb8-f856a011a8ed/resource/0b0c1493-6dbb-4650-8514-6b636c4efd53/download/web_uu_an2020.txt",
#                           col_types = "cc")

# WEB_BL_BS_SL_AN2019 <- read_csv("https://data.gov.ro/dataset/0be1e2aa-8399-4cfc-beab-bf516fcc8f16/resource/983c2a11-360c-4e42-a441-2ab13310592d/download/web_bl_bs_sl_an2019.txt",
#                                 col_types = "cc")
# WEB_IR_2019 <- read_csv("https://data.gov.ro/dataset/0be1e2aa-8399-4cfc-beab-bf516fcc8f16/resource/255501b0-3591-47ec-80c6-96d5d9cbd519/download/web_ir_an2019.txt",
#                         col_types = "cc")
# WEB_UU_AN2019 <- read_csv("https://data.gov.ro/dataset/0be1e2aa-8399-4cfc-beab-bf516fcc8f16/resource/09a63c61-b5bb-448f-ae80-fb5aaf0a175a/download/web_uu_an2019.txt",
#                           col_types = "cc")

# WEB_BL_BS_SL_AN2018 <- read_csv("https://data.gov.ro/dataset/a9c6dd10-dee2-46e9-aa3f-58dea0e50396/resource/aa694624-d14a-4cc9-9a09-9eaa7c97a0ab/download/webblbsslan2018.txt",
#                                 col_types = "cc")
# WEB_IR_2018 <- read_csv("https://data.gov.ro/dataset/a9c6dd10-dee2-46e9-aa3f-58dea0e50396/resource/a77ad9f7-aef5-4c95-8537-48003199ccdc/download/webir2018.txt",
#                         col_types = "cc")
# WEB_UU_AN2018 <- read_csv("https://data.gov.ro/dataset/a9c6dd10-dee2-46e9-aa3f-58dea0e50396/resource/86aa042a-8e42-4684-8d3f-971d849eda48/download/webuuan2018.txt",
#                           col_types = "cc")

# WEB_BL_BS_SL_AN2017 <- read_csv("https://data.gov.ro/dataset/f3c94174-4991-4d25-b183-663370908de3/resource/b00a63a0-4916-459a-a5d8-34cc68d34e86/download/webblbsslan2017.txt",
#                                 col_types = "cc")
# WEB_IR_2017 <- read_csv("https://data.gov.ro/dataset/f3c94174-4991-4d25-b183-663370908de3/resource/8c28123c-1aa3-4c62-add0-252e8c6688e7/download/webiran2017.txt",
#                         col_types = "cc")
# WEB_UU_AN2017 <- read_csv("https://data.gov.ro/dataset/f3c94174-4991-4d25-b183-663370908de3/resource/2c33ebbb-229c-427a-9752-9032a61e9613/download/webuuan2017.txt",
#                           col_types = "cc")

# WEB_BL_BS_SL_AN2016 <- read_csv("https://data.gov.ro/dataset/e6274edc-fe36-4a79-ba73-c05711b70d80/resource/a71a4687-84eb-4d5d-a315-3dc38f4ea97f/download/webblbsslan2016.txt",
#                                 col_types = "cc")
# WEB_IR_2016 <- read_csv("https://data.gov.ro/dataset/e6274edc-fe36-4a79-ba73-c05711b70d80/resource/e60fb9d2-a2aa-453d-a8fe-7b0d30d122ca/download/webiran2016.txt",
#                         col_types = "cc")
# WEB_UU_AN2016 <- read_csv("https://data.gov.ro/dataset/e6274edc-fe36-4a79-ba73-c05711b70d80/resource/7f631a71-8e41-4319-9a16-1129b02fb366/download/webuuan2016.txt",
#                           col_types = "cc")

# WEB_BL_BS_SL_AN2015 <- read_csv("https://data.gov.ro/dataset/6a021146-6a0d-4262-8b0e-2b43aa79bca7/resource/098a544a-2ced-418c-ac93-f72b4a8c1c5a/download/webblbsslan2015.txt",
#                                 col_types = "cc")
# WEB_IR_2015 <- read_csv("https://data.gov.ro/dataset/6a021146-6a0d-4262-8b0e-2b43aa79bca7/resource/7d8056aa-18bf-4385-b2e7-c5ac221a0617/download/webir2015.txt",
#                         col_types = "cc")
# WEB_UU_AN2015 <- read_csv("https://data.gov.ro/dataset/6a021146-6a0d-4262-8b0e-2b43aa79bca7/resource/f8d533be-9202-4e38-8006-ad945f1e0927/download/webuuan2015.txt",
#                           col_types = "cc")

# WEB_BL_BS_SL_AN2014 <- read_csv("https://data.gov.ro/dataset/77ea3bbe-a533-4db0-8647-d380a75a39b5/resource/5563f94a-8ca3-4739-8036-75ed9bb386aa/download/webblbsslan2014.txt",
#                                 col_types = "cc")
# WEB_IR_2014 <- read_csv("https://data.gov.ro/dataset/77ea3bbe-a533-4db0-8647-d380a75a39b5/resource/b8b8217b-55f2-408f-8279-b3ccb60f31ed/download/webiran2014.txt",
#                         col_types = "cc")
# WEB_UU_AN2014 <- read_csv("https://data.gov.ro/dataset/77ea3bbe-a533-4db0-8647-d380a75a39b5/resource/ed4aaa65-1934-4eab-a092-066b47ec5270/download/webuuan2014.txt",
#                           col_types = "cc")

# WEB_BL_BS_SL_AN2013 <- read_csv("https://data.gov.ro/dataset/f8353c0e-fee9-4aa3-b26d-be0e96c328a7/resource/d448e626-9cad-4aea-81d6-b26946d5d316/download/web_bl_bs_sl_an2013.txt",
#                                 col_types = "cc")
# WEB_IR_2013 <- read_csv("https://data.gov.ro/dataset/f8353c0e-fee9-4aa3-b26d-be0e96c328a7/resource/13dec119-4013-4689-90e0-52da4c69d3ea/download/web_ir_an2013.txt",
#                         col_types = "cc")
# WEB_UU_AN2013 <- read_csv("https://data.gov.ro/dataset/f8353c0e-fee9-4aa3-b26d-be0e96c328a7/resource/6d357eab-4419-419c-84ac-9d57858d363d/download/web_uu_an2013.txt",
#                           col_types = "cc")

# WEB_BL_BS_SL_AN2012 <- read_csv("https://data.gov.ro/dataset/f8353c0e-fee9-4aa3-b26d-be0e96c328a7/resource/991f1969-e764-400d-a0e7-2322ccc069ef/download/web_bl_bs_sl_an2012.txt",
#                                 col_types = "cc")
# WEB_IR_2012 <- read_csv("https://data.gov.ro/dataset/f8353c0e-fee9-4aa3-b26d-be0e96c328a7/resource/3bc08376-26f0-47ec-8674-28ba2b8b329e/download/web_ir_an2012.txt",
#                         col_types = "cc")
# WEB_UU_AN2012 <- read_csv("https://data.gov.ro/dataset/f8353c0e-fee9-4aa3-b26d-be0e96c328a7/resource/f02ee4d8-a9b9-4095-9695-35927edbb605/download/web_uu_2012.txt",
#                           col_types = "cc")
