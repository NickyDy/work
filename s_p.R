library(tidyverse)
library(nanoparquet)
library(fs)
library(readxl)

options(scipen = 100)

portugal <- read_parquet("work/portugal.parquet")
portugal_na <- read_parquet("work/portugal_na.parquet")
portugal_csv <- read_csv("work/portugal.csv")

portugal <- left_join(balance_sheet, income_statement %>% select(-c(2, 4:7)), by = c("year", "sp_entity_id"))

glimpse(portugal)

read_excel_col <- function(file){
  read_excel(file, col_names = c("entity_name", "entity_id", "geograhphy", "company_status",
                                 "company_type", "industry", "total_rev", "net_inc", "total_assets",
                                 "total_debt", "total_equity"), skip = 7, na = "NA")
}

read_excel_names <- function(file){
  read_excel(file, 
             skip = 4, 
             na = "NA") %>% 
    slice(-c(1:2)) %>% 
    janitor::clean_names()
}

files <- dir_ls("~/Downloads", regexp = "xlsx")
df <- map(files, read_excel_names) %>%
  set_names(basename) %>%
  list_rbind(names_to = "year") %>%
  mutate(year = str_extract(year, "\\d+")) %>% 
  mutate(across(1:7, as.character)) %>% 
  mutate(across(-c(1:7), as.numeric)) %>% 
  mutate(across(where(is.numeric), \(x) round(x, 2)))

glimpse(portugal)

portugal %>% count(sp_entity_id) %>% view


plot_id("8026915")

plot_id <- function(number) {
  title <- portugal %>%
    filter(sp_entity_id == number)
  
  portugal %>%
    filter(sp_entity_id == number) %>%
    pivot_longer(c(pcd_total_current_assets, pcd_total_assets, pcd_total_common_equity, pcd_total_equity,
                   pcd_total_liab_equity, pcd_total_ca_reported, pcd_total_assets_reported,
                   pcd_total_stock_holders_equity_reported, pcd_total_liabilities_reported,
                   pcd_total_current_liab, pcd_total_liab, pcd_retained_earnings, pcd_total_cl_reported)) %>%
    drop_na(value) %>%
    mutate(col = value > 0, name = fct_inorder(name)) %>%
    ggplot(aes(as.numeric(year), value)) +
    geom_point(show.legend = F) +
    geom_line(linetype = 2, linewidth = 0.2) +
    scale_x_continuous(n.breaks = 10) +
    labs(x = "Година", y = "Стойност",
         title = paste0("ENTITY NAME: ", unique(title$sp_entity_name))) +
    facet_wrap(vars(name), scales = "free_y", ncol = 3) +
    theme(text = element_text(size = 16))
}

portugal %>%
  map_dfr(~ sum(is.na(.))) %>%
  pivot_longer(everything()) %>%
  mutate(perc_na = round(value / nrow(portugal) * 100, 2)) %>%
  arrange(perc_na) %>% 
  #filter(perc_na == 100) %>% 
  print(n = Inf)

portugal_na %>%
  reframe(across(everything(), ~ round(sum(is.na(.) / n() * 100), 3)), .by = year) %>%
  as_tibble() %>%
  pivot_longer(-year, names_to = "index", values_to = "perc_na") %>%
  arrange(year, perc_na) %>%
  select(index, year, perc_na) %>%
  pivot_wider(names_from = year, values_from = perc_na) %>% 
  print(n = Inf)

#write_parquet(portugal_na, "work/portugal_na.parquet")
#write_csv(portugal, "work/portugal.csv")
