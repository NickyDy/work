library(tidyverse)
library(nanoparquet)
options(scipen = 100)

rps <- read_parquet("work/rps.parquet")
rgfi <- read_parquet("work/rgfi.parquet")

comp <- rgfi %>% 
  select(pla_13, pla_11, liabilities_a7) %>% 
  mutate(comp = pla_13 == pla_11)

rps %>% filter(mb == 3586243, rb == 0000) %>% view

glimpse(rps)
rps %>% count(rb, mb, sort = T) %>% view

rgfi_year_na <- rgfi %>% 
  reframe(across(everything(), ~ round(sum(is.na(.) / n() * 100), 3)), .by = year) %>% 
  as_tibble() %>%
  pivot_longer(-year, names_to = "index", values_to = "perc_na") %>% 
  arrange(year, perc_na) %>%
  select(index, year, perc_na) %>% 
  pivot_wider(names_from = year, values_from = perc_na)

df_na <- df %>%
  map_dfr(~ sum(is.na(.))) %>% 
  pivot_longer(everything()) %>%
  mutate(perc_na = round(value / nrow(rgfi) * 100, 2)) %>% 
  arrange(perc_na)

rgfi %>% 
  #mutate(mb = paste0("00", mb)) %>% 
  filter(mb == "1968823") %>%
  pivot_longer(-c(1:4)) %>% 
  drop_na(value) %>% 
  ggplot(aes(dat_predaje, value)) +
  geom_line() +
  geom_point() +
  scale_x_date(date_breaks = "1 year", date_labels = "%Y") +
  facet_wrap(vars(name), scales = "free_y", ncol = 5)
#------------------------------------------
df %>% count(oib, sort = T)

glimpse(rgfi_2023)

rps <- rps %>% 
  filter(rb == 0000) %>% 
  distinct()

df_croatia <- rgfi %>% 
  left_join(., rps, by = "mb")

df_croatia <- df_croatia %>% 
  select(oib, mb, dat_predaje, year, rb:oblik_vlasništva_naziv, everything())

write_parquet(rps, "work/rps.parquet")
write_parquet(rgfi, "work/rgfi.parquet")
write_csv(rgfi, "work/rgfi.csv")
#---------------------------------------
rgfi_2023 <- read_csv2("work/RGFI_javna_objava_2023.csv", 
                       quote = "\'", na = "\"\"") %>% 
  janitor::clean_names() %>% 
  mutate(across(everything(), ~str_remove_all(., '\"'))) %>% 
  filter(vr_izvj == 10, 
         str_detect(naz_izvjestaja2, "01.01.2023. - 31.12.2023."),
         ozn_kons == 0) %>% 
  select(oib, mb, dat_predaje, contains(c("tg1", "tg2"))) %>%
  mutate(dat_predaje = dmy(dat_predaje), year = 2023) %>% 
  mutate(across(-dat_predaje, as.numeric))

rgfi_2022 <- read_csv2("work/RGFI_javna_objava_2022.csv", 
                       quote = "\'", na = "\"\"") %>% 
  janitor::clean_names() %>% 
  mutate(across(everything(), ~str_remove_all(., '\"'))) %>% 
  filter(vr_izvj == 10, 
         str_detect(naz_izvjestaja2, "01.01.2022. - 31.12.2022."),
         ozn_kons == 0) %>% 
  select(oib, mb, dat_predaje, contains(c("tg1", "tg2"))) %>%
  mutate(dat_predaje = dmy(dat_predaje), year = 2022) %>% 
  mutate(across(-dat_predaje, as.numeric))

rgfi_2021 <- read_csv2("work/RGFI_javna_objava_2021.csv", 
                       quote = "\'", na = "\"\"") %>% 
  janitor::clean_names() %>% 
  mutate(across(everything(), ~str_remove_all(., '\"'))) %>% 
  filter(vr_izvj == 10, 
         str_detect(naz_izvjestaja2, "01.01.2021. - 31.12.2021."),
         ozn_kons == 0) %>% 
  select(oib, mb, dat_predaje, contains(c("tg1", "tg2"))) %>%
  mutate(dat_predaje = dmy(dat_predaje), year = 2021) %>% 
  mutate(across(-dat_predaje, as.numeric))

rgfi_2020 <- read_csv2("work/RGFI_javna_objava_2020.csv", 
                       quote = "\'", na = "\"\"") %>% 
  janitor::clean_names() %>% 
  mutate(across(everything(), ~str_remove_all(., '\"'))) %>% 
  filter(vr_izvj == 10, 
         str_detect(naz_izvjestaja2, "01.01.2020. - 31.12.2020."),
         ozn_kons == 0) %>% 
  select(oib, mb, dat_predaje, contains(c("tg1", "tg2"))) %>%
  mutate(dat_predaje = dmy(dat_predaje), year = 2020) %>%
  rename(aop_tg1_83 = aop_tg1_81, aop_tg1_86 = aop_tg1_84,
         aop_tg1_89 = aop_tg1_87, aop_tg1_90 = aop_tg1_88,
         aop_tg1_97 = aop_tg1_95, aop_tg1_109 = aop_tg1_107,
         aop_tg1_124 = aop_tg1_122, aop_tg1_125 = aop_tg1_123,
         aop_tg1_126 = aop_tg1_124, aop_tg2_127 = aop_tg2_125,
         aop_tg2_133 = aop_tg2_131, aop_tg2_156 = aop_tg2_154,
         aop_tg2_167 = aop_tg2_165, aop_tg2_175 = aop_tg2_173,
         aop_tg2_176 = aop_tg2_174, aop_tg2_177 = aop_tg2_175,
         aop_tg2_178 = aop_tg2_176, aop_tg2_179 = aop_tg2_177,
         aop_tg2_180 = aop_tg2_178, aop_tg2_181 = aop_tg2_179,
         aop_tg2_184 = aop_tg2_182, aop_tg2_185 = aop_tg2_183,
         aop_tg2_205 = aop_tg2_203, aop_tg2_223 = aop_tg2_213,
         aop_tg2_224 = aop_tg2_214) %>%
  mutate(across(-dat_predaje, as.numeric))

rgfi_2019 <- read_csv2("work/RGFI_javna_objava_2019.csv", 
                       quote = "\'", na = "\"\"") %>% 
  janitor::clean_names() %>% 
  mutate(across(everything(), ~str_remove_all(., '\"'))) %>% 
  filter(vr_izvj == 10, 
         str_detect(naz_izvjestaja2, "01.01.2019. - 31.12.2019."),
         ozn_kons == 0) %>% 
  select(oib, mb, dat_predaje, contains(c("tg1", "tg2"))) %>%
  mutate(dat_predaje = dmy(dat_predaje), year = 2019) %>%
  rename(aop_tg1_83 = aop_tg1_81, aop_tg1_86 = aop_tg1_84,
         aop_tg1_89 = aop_tg1_87, aop_tg1_90 = aop_tg1_88,
         aop_tg1_97 = aop_tg1_95, aop_tg1_109 = aop_tg1_107,
         aop_tg1_124 = aop_tg1_122, aop_tg1_125 = aop_tg1_123,
         aop_tg1_126 = aop_tg1_124, aop_tg2_127 = aop_tg2_125,
         aop_tg2_133 = aop_tg2_131, aop_tg2_156 = aop_tg2_154,
         aop_tg2_167 = aop_tg2_165, aop_tg2_175 = aop_tg2_173,
         aop_tg2_176 = aop_tg2_174, aop_tg2_177 = aop_tg2_175,
         aop_tg2_178 = aop_tg2_176, aop_tg2_179 = aop_tg2_177,
         aop_tg2_180 = aop_tg2_178, aop_tg2_181 = aop_tg2_179,
         aop_tg2_184 = aop_tg2_182, aop_tg2_185 = aop_tg2_183,
         aop_tg2_205 = aop_tg2_203, aop_tg2_223 = aop_tg2_213,
         aop_tg2_224 = aop_tg2_214) %>%
  mutate(across(-dat_predaje, as.numeric))

rgfi_2018 <- read_csv2("work/RGFI_javna_objava_2018.csv", 
                       quote = "\'", na = "\"\"") %>% 
  janitor::clean_names() %>% 
  mutate(across(everything(), ~str_remove_all(., '\"'))) %>% 
  filter(vr_izvj == 10, 
         str_detect(naz_izvjestaja2, "01.01.2018. - 31.12.2018."),
         ozn_kons == 0) %>% 
  select(oib, mb, dat_predaje, contains(c("tg1", "tg2"))) %>%
  mutate(dat_predaje = dmy(dat_predaje), year = 2018) %>%
  rename(aop_tg1_83 = aop_tg1_81, aop_tg1_86 = aop_tg1_84,
         aop_tg1_89 = aop_tg1_87, aop_tg1_90 = aop_tg1_88,
         aop_tg1_97 = aop_tg1_95, aop_tg1_109 = aop_tg1_107,
         aop_tg1_124 = aop_tg1_122, aop_tg1_125 = aop_tg1_123,
         aop_tg1_126 = aop_tg1_124, aop_tg2_127 = aop_tg2_125,
         aop_tg2_133 = aop_tg2_131, aop_tg2_156 = aop_tg2_154,
         aop_tg2_167 = aop_tg2_165, aop_tg2_175 = aop_tg2_173,
         aop_tg2_176 = aop_tg2_174, aop_tg2_177 = aop_tg2_175,
         aop_tg2_178 = aop_tg2_176, aop_tg2_179 = aop_tg2_177,
         aop_tg2_180 = aop_tg2_178, aop_tg2_181 = aop_tg2_179,
         aop_tg2_184 = aop_tg2_182, aop_tg2_185 = aop_tg2_183,
         aop_tg2_205 = aop_tg2_203, aop_tg2_223 = aop_tg2_213,
         aop_tg2_224 = aop_tg2_214) %>%
  mutate(across(-dat_predaje, as.numeric))

rgfi_2017 <- read_csv2("work/RGFI_javna_objava_2017.csv", 
                       quote = "\'", na = "\"\"") %>% 
  janitor::clean_names() %>% 
  mutate(across(everything(), ~str_remove_all(., '\"'))) %>% 
  filter(vr_izvj == 10, 
         str_detect(naz_izvjestaja2, "01.01.2017. - 31.12.2017."),
         ozn_kons == 0) %>% 
  select(oib, mb, dat_predaje, contains(c("tg1", "tg2"))) %>%
  mutate(dat_predaje = dmy(dat_predaje), year = 2017) %>%
  rename(aop_tg1_83 = aop_tg1_81, aop_tg1_86 = aop_tg1_84,
         aop_tg1_89 = aop_tg1_87, aop_tg1_90 = aop_tg1_88,
         aop_tg1_97 = aop_tg1_95, aop_tg1_109 = aop_tg1_107,
         aop_tg1_124 = aop_tg1_122, aop_tg1_125 = aop_tg1_123,
         aop_tg1_126 = aop_tg1_124, aop_tg2_127 = aop_tg2_125,
         aop_tg2_133 = aop_tg2_131, aop_tg2_156 = aop_tg2_154,
         aop_tg2_167 = aop_tg2_165, aop_tg2_175 = aop_tg2_173,
         aop_tg2_176 = aop_tg2_174, aop_tg2_177 = aop_tg2_175,
         aop_tg2_178 = aop_tg2_176, aop_tg2_179 = aop_tg2_177,
         aop_tg2_180 = aop_tg2_178, aop_tg2_181 = aop_tg2_179,
         aop_tg2_184 = aop_tg2_182, aop_tg2_185 = aop_tg2_183,
         aop_tg2_205 = aop_tg2_203, aop_tg2_223 = aop_tg2_213,
         aop_tg2_224 = aop_tg2_214) %>%
  mutate(across(-dat_predaje, as.numeric))

rgfi_2016 <- read_csv2("work/RGFI_javna_objava_2016.csv", 
                       quote = "\'", na = "\"\"") %>% 
  janitor::clean_names() %>% 
  mutate(across(everything(), ~str_remove_all(., '\"'))) %>% 
  filter(vr_izvj == 10, 
         str_detect(naz_izvjestaja2, "01.01.2016. - 31.12.2016."),
         ozn_kons == 0) %>% 
  select(oib, mb, dat_predaje, contains(c("tg1", "tg2"))) %>%
  mutate(dat_predaje = dmy(dat_predaje), year = 2016) %>%
  rename(aop_tg1_83 = aop_tg1_81, aop_tg1_86 = aop_tg1_84,
         aop_tg1_89 = aop_tg1_87, aop_tg1_90 = aop_tg1_88,
         aop_tg1_97 = aop_tg1_95, aop_tg1_109 = aop_tg1_107,
         aop_tg1_124 = aop_tg1_122, aop_tg1_125 = aop_tg1_123,
         aop_tg1_126 = aop_tg1_124, aop_tg2_127 = aop_tg2_125,
         aop_tg2_133 = aop_tg2_131, aop_tg2_156 = aop_tg2_154,
         aop_tg2_167 = aop_tg2_165, aop_tg2_175 = aop_tg2_173,
         aop_tg2_176 = aop_tg2_174, aop_tg2_177 = aop_tg2_175,
         aop_tg2_178 = aop_tg2_176, aop_tg2_179 = aop_tg2_177,
         aop_tg2_180 = aop_tg2_178, aop_tg2_181 = aop_tg2_179,
         aop_tg2_184 = aop_tg2_182, aop_tg2_185 = aop_tg2_183,
         aop_tg2_205 = aop_tg2_203, aop_tg2_223 = aop_tg2_213,
         aop_tg2_224 = aop_tg2_214) %>%
  mutate(across(-dat_predaje, as.numeric))

rgfi_2015 <- read_csv2("work/RGFI_javna_objava_2015.csv", 
                       quote = "\'", na = "\"\"") %>% 
  janitor::clean_names() %>% 
  mutate(across(everything(), ~str_remove_all(., '\"'))) %>% 
  filter(vr_izvj == 10, 
         str_detect(naz_izvjestaja2, "01.01.2015. - 31.12.2015."),
         ozn_kons == 0) %>% 
  select(oib, mb, dat_predaje, contains(c("tg1", "tg2"))) %>%
  mutate(dat_predaje = dmy(dat_predaje), year = 2015) %>%
  rename(aop_tg1_31 = aop_tg1_29, aop_tg1_36 = aop_tg1_33,
         aop_tg1_37 = aop_tg1_34, aop_tg1_38 = aop_tg1_35,
         aop_tg1_46 = aop_tg1_43, aop_tg1_53 = aop_tg1_50,
         aop_tg1_63 = aop_tg1_58, aop_tg1_64 = aop_tg1_59,
         aop_tg1_65 = aop_tg1_60, aop_tg1_66 = aop_tg1_61,
         aop_tg1_67 = aop_tg1_62, aop_tg1_68 = aop_tg1_63,
         aop_tg1_69 = aop_tg1_64, aop_tg1_70 = aop_tg1_65,
         aop_tg1_76 = aop_tg1_71, aop_tg1_83 = aop_tg1_72,
         aop_tg1_86 = aop_tg1_75, aop_tg1_89 = aop_tg1_78,
         aop_tg1_90 = aop_tg1_79, aop_tg1_97 = aop_tg1_83,
         aop_tg1_109 = aop_tg1_93, aop_tg1_124 = aop_tg1_106,
         aop_tg1_125 = aop_tg1_107, aop_tg1_126 = aop_tg1_108,
         aop_tg2_127 = aop_tg2_111, aop_tg2_133 = aop_tg2_114,
         aop_tg2_156 = aop_tg2_131, aop_tg2_167 = aop_tg2_137,
         aop_tg2_179 = aop_tg2_146, aop_tg2_180 = aop_tg2_147,
         aop_tg2_181 = aop_tg2_148, aop_tg2_184 = aop_tg2_151,
         aop_tg2_185 = aop_tg2_152, aop_tg2_202 = aop_tg2_157,
         aop_tg2_205 = aop_tg2_158, aop_tg2_223 = aop_tg2_167,
         aop_tg2_224 = aop_tg2_168) %>%
  mutate(across(-dat_predaje, as.numeric))

rgfi_2014 <- read_csv2("work/RGFI_javna_objava_2014.csv", 
                       quote = "\'", na = "\"\"") %>% 
  janitor::clean_names() %>% 
  mutate(across(everything(), ~str_remove_all(., '\"'))) %>% 
  filter(vr_izvj == 10, 
         str_detect(naz_izvjestaja2, "01.01.2014. - 31.12.2014."),
         ozn_kons == 0) %>% 
  select(oib, mb, dat_predaje, contains(c("tg1", "tg2"))) %>%
  mutate(dat_predaje = dmy(dat_predaje), year = 2014) %>%
  rename(aop_tg1_31 = aop_tg1_29, aop_tg1_36 = aop_tg1_33,
         aop_tg1_37 = aop_tg1_34, aop_tg1_38 = aop_tg1_35,
         aop_tg1_46 = aop_tg1_43, aop_tg1_53 = aop_tg1_50,
         aop_tg1_63 = aop_tg1_58, aop_tg1_64 = aop_tg1_59,
         aop_tg1_65 = aop_tg1_60, aop_tg1_66 = aop_tg1_61,
         aop_tg1_67 = aop_tg1_62, aop_tg1_68 = aop_tg1_63,
         aop_tg1_69 = aop_tg1_64, aop_tg1_70 = aop_tg1_65,
         aop_tg1_76 = aop_tg1_71, aop_tg1_83 = aop_tg1_72,
         aop_tg1_86 = aop_tg1_75, aop_tg1_89 = aop_tg1_78,
         aop_tg1_90 = aop_tg1_79, aop_tg1_97 = aop_tg1_83,
         aop_tg1_109 = aop_tg1_93, aop_tg1_124 = aop_tg1_106,
         aop_tg1_125 = aop_tg1_107, aop_tg1_126 = aop_tg1_108,
         aop_tg2_127 = aop_tg2_111, aop_tg2_133 = aop_tg2_114,
         aop_tg2_156 = aop_tg2_131, aop_tg2_167 = aop_tg2_137,
         aop_tg2_179 = aop_tg2_146, aop_tg2_180 = aop_tg2_147,
         aop_tg2_181 = aop_tg2_148, aop_tg2_184 = aop_tg2_151,
         aop_tg2_185 = aop_tg2_152, aop_tg2_202 = aop_tg2_157,
         aop_tg2_205 = aop_tg2_158, aop_tg2_223 = aop_tg2_167,
         aop_tg2_224 = aop_tg2_168) %>%
  mutate(across(-dat_predaje, as.numeric))

rgfi <- bind_rows(rgfi_2023, rgfi_2022, rgfi_2021, rgfi_2020, rgfi_2019, rgfi_2018,
                  rgfi_2017, rgfi_2016, rgfi_2015, rgfi_2014) %>% 
  select(oib, mb, dat_predaje, year, assets_a = aop_tg1_2, assets_b1 = aop_tg1_3,
         assets_b2 = aop_tg1_10, assets_b3 = aop_tg1_20, assets_c = aop_tg1_37,
         assets_c1 = aop_tg1_38, assets_c2 = aop_tg1_46, assets_c3 = aop_tg1_53, assets_c4 = aop_tg1_63,
         assets_d = aop_tg1_64, assets_e = aop_tg1_65, liabilities_a = aop_tg1_67,
         liabilities_a1 = aop_tg1_68, liabilities_a2 = aop_tg1_69, liabilities_a3 = aop_tg1_70,
         liabilities_a6 = aop_tg1_83, liabilities_a7 = aop_tg1_86, liabilities_c = aop_tg1_97,
         liabilities_d = aop_tg1_109, liabilities_e = aop_tg1_124, liabilities_f = aop_tg1_125,
         pla_1 = aop_tg2_127, pla_2 = aop_tg2_133, pla_3 = aop_tg2_156, pla_4 = aop_tg2_167,
         pla_9 = aop_tg2_179, pla_10 = aop_tg2_180,
         pla_11 = aop_tg2_181, pla_12 = aop_tg2_184, pla_13 = aop_tg2_185) %>% 
  distinct()

glimpse(rgfi)

write_csv(rgfi, "work/rgfi.csv")
write_parquet(rgfi, "work/rgfi.parquet")
