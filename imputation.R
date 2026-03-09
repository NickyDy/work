library(tidyverse)
library(nanoparquet)
library(naniar)

# Прочитане и филтриране на данните
romania <- read_parquet("romania.parquet") %>% 
  filter(year == "2014") %>%
  mutate(across(where(is.character), as.factor))

# Общ поглед върху данните
glimpse(romania)

# Липсващи данни преди изчислението
romania %>% miss_var_summary() %>% 
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
