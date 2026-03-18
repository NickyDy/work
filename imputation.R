library(tidyverse)
library(nanoparquet)
library(naniar)

# Прочитане и филтриране на данните
romania <- read_parquet("work/parquet/romania.parquet") %>% 
  mutate(across(where(is.character), as.factor))

# Общ поглед върху данните
glimpse(romania)

romania %>% 
  filter(year == "2024") %>%
  select(assets_c4, assets_c, liabilities_a1, pla_11l, pla_13l,
         pla_13p, pla_11p, liabilities_a, net_turnover, pla_9,
         pla_10, assets_c2, assets_b, employees, assets_c1,
         liabilities_f) %>%
  drop_na() %>% 
  cor() %>% round(3) %>% as.data.frame() %>% 
  rownames_to_column(var = "variables (2024)") %>% 
  as_tibble()

romania %>% 
  select(cui, year, liabilities_f) %>% 
  pivot_wider(names_from = year, values_from = liabilities_f) %>% 
  select(-cui) %>% 
  drop_na() %>% 
  cor() %>% round(3) %>% as.data.frame() %>% 
  rownames_to_column(var = "liabilities_f") %>% 
  as_tibble()

# Липсващи данни преди изчислението
romania %>% 
  #filter(year == "2015") %>%
  miss_var_summary() %>% 
  arrange(n_miss) %>% print(n = Inf)

library(missForest)
library(doParallel)

# Брой ядра на процесора, които да се използват в изчислението
registerDoParallel(4) 

# Изчисление на липсващите данни
imputed <- missForest(romania, 
                      parallelize = "forests", 
                      verbose = TRUE)

# Липсващи данни след изчислението
imputed %>% pluck("ximp") %>% 
  miss_var_summary() %>% arrange(n_miss) %>% 
  print(n = Inf)

# Преглед на изчислените данни
romania_imputed <- imputed %>% pluck("ximp")
romania_imputed %>% view

# Запазване на изчислените данни
write_parquet(romania_imputed, "romania_2014.parquet")

#====================
# library(tidymodels)
# romania %>% miss_var_summary() %>% print(n = Inf)
# 
# impute_knn <- recipe(year + cui + source + nace2 + liabilities_f + liabilities_a ~ ., data = romania) %>%
#   step_impute_knn(all_predictors())
# 
# impute_bag <- recipe(year + cui + source + nace2 + liabilities_f + liabilities_a ~ ., data = romania) %>%
#   step_impute_bag(all_predictors())
# 
# imputed <- prep(impute_knn) %>% bake(new_data = NULL)
# 
# imputed %>% miss_var_summary() %>% print(n = Inf)
