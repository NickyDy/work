library(tidyverse)
library(fs)
library(readxl)
options(scipen = 100)

portugal <- read_csv("work/portugal.csv")

read_excel_col <- function(file){
  read_excel(file, col_names = c("entity_name", "entity_id", "geograhphy", "company_status",
                                 "company_type", "industry", "total_rev", "net_inc", "total_assets",
                                 "total_debt", "total_equity"), skip = 7, na = "NA")
}

read_excel_names <- function(file){
  read_excel(file, skip = 4, na = "NA") %>% 
    slice(-c(1:2)) %>% 
    janitor::clean_names()
}

files <- dir_ls("~/Downloads", regexp = "xlsx")
df <- map(files, read_excel_names) %>%
  set_names(basename) %>%
  list_rbind(names_to = "year") %>%
  mutate(year = str_extract(year, "\\d+")) %>% 
  mutate(across(1:6, as.character)) %>% 
  mutate(across(-c(1:6), as.numeric)) %>% 
  mutate(across(where(is.numeric), \(x) round(x, 2)))

glimpse(df)
portugal %>%
  drop_na(total_rev, net_inc, total_assets, 
          total_debt, total_equity) %>% 
  count(entity_id) %>% 
  filter(n >= 9) %>% 
  view

plot_id("4434302")

plot_id <- function(number) {
  title <- portugal %>%
    filter(entity_id == number)
  
  portugal %>%
    filter(entity_id == number) %>%
    pivot_longer(total_rev:total_equity) %>%
    drop_na() %>%
    mutate(col = value > 0, name = fct_inorder(name)) %>%
    ggplot(aes(year, value, fill = col, group = name)) +
    geom_point(show.legend = F) +
    geom_line(linetype = 2, linewidth = 0.2) +
    scale_x_continuous(n.breaks = 10) +
    labs(x = "Година", y = "Стойност",
         title = paste0("ENTITY ID: ", unique(title$entity_id))) +
    facet_wrap(vars(name), scales = "free_y", ncol = 3) +
    theme(text = element_text(size = 16))
}

df %>%
  map_dfr(~ sum(is.na(.))) %>%
  pivot_longer(everything()) %>%
  mutate(perc_na = round(value / nrow(df) * 100, 2)) %>%
  arrange(perc_na) %>% 
  print(n = Inf)

df %>%
  reframe(across(everything(), ~ round(sum(is.na(.) / n() * 100), 3)), .by = year) %>%
  as_tibble() %>%
  pivot_longer(-year, names_to = "index", values_to = "perc_na") %>%
  arrange(year, perc_na) %>%
  select(index, year, perc_na) %>%
  pivot_wider(names_from = year, values_from = perc_na) %>% 
  print(n = Inf)

#write_csv(df, "work/portugal.csv")
