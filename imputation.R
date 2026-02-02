library(tidyverse)
library(nanoparquet)
library(naniar)

options(scipen = 100)

romania <- read_parquet("work/parquet/romania.parquet") %>% 
  slice_sample(n = 10000) %>% 
  mutate(across(where(is.character), as.factor))

glimpse(romania)

library(missForest)
library(doParallel)
registerDoParallel(4)

romania %>% miss_var_summary() %>% print(n = Inf)

imputed <- missForest(romania, parallelize = "forests", verbose = TRUE)

imputed[["ximp"]] %>% miss_var_summary() %>% print(n = Inf)

romania_imputed <- imputed[["ximp"]] %>% view

library(tidymodels)

romania %>% miss_var_summary() %>% print(n = Inf)

impute_rec <- recipe(year + cui + source + nace2 + liabilities_f + liabilities_a ~ ., data = romania) %>%
  step_impute_knn(all_predictors())

imputed <- prep(impute_rec) %>% bake(new_data = NULL)

imputed %>% miss_var_summary() %>% print(n = Inf)