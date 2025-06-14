library(tidyverse)
library(nanoparquet)
library(readxl)
library(scales)

options(scipen = 100)
space_s <- function (x, accuracy = NULL, scale = 1, prefix = "", suffix = "", 
                     big.mark = " ", decimal.mark = ".", trim = TRUE, digits, 
                     ...)
{
  if (!missing(digits)) {
    lifecycle::deprecate_stop(when = "1.0.0", what = "comma(digits)", 
                              with = "comma(accuracy)")
  }
  number(x = x, accuracy = accuracy, scale = scale, prefix = prefix, 
         suffix = suffix, big.mark = big.mark, decimal.mark = decimal.mark, 
         trim = trim, ...)
}

programs <- read_parquet("work/programs.parquet")
projects_df <- read_parquet("work/projects_df.parquet") %>% janitor::clean_names() %>% distinct()

glimpse(programs)

programs %>% count(end_date_p, sort = T) %>% view

projects_df <- left_join(projects_df, projects_xml, by = c("Номер на проектно предложение" = "project_id"))

projects_df %>% count(programa, sort = T) %>% view
projects_df %>% filter(Местонахождение == "гр.Ямбол") %>% 
  mutate(Бенефициент = str_to_upper(Бенефициент)) %>% 
  count(Бенефициент, `Номер на проектно предложение`, wt = `Реално изплатени суми`) %>% view

no <- "BG-RRP-1.001-0002"

title <- projects_df %>% filter(`Номер на проектно предложение` %in% c(no))

projects_df %>% 
  filter(`Номер на проектно предложение` %in% c(no)) %>%
  pivot_longer(`Обща стойност`:`Реално изплатени суми`) %>%
  mutate(col = value > 0, name = fct_inorder(name)) %>% 
  ggplot(aes(value, name, fill = col)) +
  geom_col(show.legend = F) +
  geom_text(aes(label = paste0(space_s(value), " лв")), hjust = -0.05, size = 4) +
  scale_x_continuous(expand = expansion(mult = c(0.01, 0.1))) +
  scale_fill_manual(values = c("TRUE" = "#00BFC4", "FALSE" = "#F8766D")) +
  theme(text = element_text(size = 16), axis.ticks.x = element_blank(),
        axis.text.x = element_blank()) +
  labs(x = "Изплатена сума (лв)", y = NULL,
       title = paste0("Бенефициент: ",  title$Бенефициент,"\n",
                      "Име на проект: ", str_wrap(title$`Наименование на проекта`, width = 100),"\n",
                      "Номер на проект: ", title$`Номер на проектно предложение`,"\n",
                      #"Начална дата: ", title,"\n",
                      #"Крайна дата: ", title$end_date,"\n",
                      "Продължителност в месеци: ", title$`Продължителност (месеци)`))

programi %>% count(nomer_na_proektno_predlozenie, sort = T) %>% view
glimpse(projects_df)

write_parquet(programs, "work/programs.parquet")
write_csv(programs, "work/programs.csv")
#-----------------------------------------------------
transport <- read_excel("work/transport.xlsx") %>% 
  mutate(across(c(1:9, 15), as.character)) %>% mutate(across(10:14, as.numeric))
transport_xml <- read_csv("work/transport.csv") %>% 
  select(!starts_with("Partners")) %>% 
  select(1:9, DurationInMonths, Description, Status, everything())
transport_df <- left_join(transport, transport_xml, 
                          by = c("Номер на проектно предложение" = "Id")) %>%
  pivot_longer(27:92, names_to = "payment", values_to = "value_paid") %>% 
  select(-DateofDecisionforFunding, 
         -`ProjectBeneficiary/EntityId`, 
         -`BeneficiaryFunding/_xsi:nil`)

okolna_sreda <- read_excel("work/okolna_sreda.xlsx", skip = 18) %>% slice(-c(392:397)) %>% 
  mutate(across(c(1:9, 15), as.character)) %>% mutate(across(10:14, as.numeric))
okolna_sreda_xml <- read_csv("work/okolna_sreda.csv") %>%
  select(!starts_with("Partners")) %>% 
  select(1:9, DurationInMonths, Description, Status, everything())
okolna_sreda_df <- left_join(okolna_sreda, okolna_sreda_xml, 
                             by = c("Номер на проектно предложение" = "Id")) %>%
  pivot_longer(27:64, names_to = "payment", values_to = "value_paid") %>% 
  select(-DateofDecisionforFunding, 
         -`ProjectBeneficiary/EntityId`, 
         -`BeneficiaryFunding/_xsi:nil`)

regioni_v_rastej <- read_excel("work/regioni_v_rastej.xlsx", skip = 18) %>% slice(-c(825:830)) %>% 
  mutate(across(c(1:9, 15), as.character)) %>% mutate(across(10:14, as.numeric))
regioni_v_rastej_xml <- read_csv("work/regioni_v_rastej.csv") %>%
  select(!starts_with("Partners")) %>% 
  select(1:9, DurationInMonths, Description, Status, everything())
regioni_v_rastej_df <- left_join(regioni_v_rastej, regioni_v_rastej_xml, 
                             by = c("Номер на проектно предложение" = "Id")) %>%
  pivot_longer(27:801, names_to = "payment", values_to = "value_paid") %>% 
  select(-DateofDecisionforFunding, 
         -`ProjectBeneficiary/EntityId`, 
         -`BeneficiaryFunding/_xsi:nil`)

inovacii <- read_excel("work/inovacii.xlsx", skip = 18) %>% slice(-c(35333:35338)) %>% 
  mutate(across(c(1:9, 15), as.character)) %>% mutate(across(10:14, as.numeric))
inovacii_xml <- read_csv("work/inovacii.csv") %>%
  select(!starts_with("Partners")) %>% 
  select(1:9, DurationInMonths, Description, Status, everything())
inovacii_df <- left_join(inovacii, inovacii_xml, 
                                 by = c("Номер на проектно предложение" = "Id")) %>%
  pivot_longer(27:60, names_to = "payment", values_to = "value_paid") %>% 
  select(-DateofDecisionforFunding, 
         -`ProjectBeneficiary/EntityId`, 
         -nil)

nauka <- read_excel("work/nauka.xlsx", skip = 18) %>% slice(-c(377:382)) %>% 
  mutate(across(c(1:9, 15), as.character)) %>% mutate(across(10:14, as.numeric))
nauka_xml <- read_csv("work/nauka.csv") %>%
  select(!starts_with("Partners")) %>% 
  select(1:9, DurationInMonths, Description, Status, everything())
nauka_df <- left_join(nauka, nauka_xml, 
                         by = c("Номер на проектно предложение" = "Id")) %>%
  pivot_longer(27:113, names_to = "payment", values_to = "value_paid") %>% 
  select(-DateofDecisionforFunding, 
         -`ProjectBeneficiary/EntityId`, 
         -`BeneficiaryFunding/_xsi:nil`)

r4r <- read_excel("work/r4r.xlsx", skip = 18) %>% slice(-c(4404:4409)) %>% 
  mutate(across(c(1:9, 15), as.character)) %>% mutate(across(10:14, as.numeric))
r4r_xml <- read_csv("work/r4r.csv") %>%
  select(!starts_with("Partners")) %>% 
  select(1:9, DurationInMonths, Description, Status, everything())
r4r_df <- left_join(r4r, r4r_xml, 
                      by = c("Номер на проектно предложение" = "Id")) %>%
  pivot_longer(27:118, names_to = "payment", values_to = "value_paid") %>% 
  select(-DateofDecisionforFunding, 
         -`ProjectBeneficiary/EntityId`, 
         -`BeneficiaryFunding/_xsi:nil`)

dobro_upravlenie <- read_excel("work/dobro_upravlenie.xlsx", skip = 18) %>% slice(-c(707:712)) %>% 
  mutate(across(c(1:9, 15), as.character)) %>% mutate(across(10:14, as.numeric))
dobro_upravlenie_xml <- read_csv("work/dobro_upravlenie.csv") %>%
  select(!starts_with("Partners")) %>% 
  select(1:9, DurationInMonths, Description, Status, everything())
dobro_upravlenie_df <- left_join(dobro_upravlenie, dobro_upravlenie_xml, 
                    by = c("Номер на проектно предложение" = "Id")) %>%
  pivot_longer(27:39, names_to = "payment", values_to = "value_paid") %>% 
  select(-DateofDecisionforFunding, 
         -`ProjectBeneficiary/EntityId`, 
         -`BeneficiaryFunding/_xsi:nil`)

hrani <- read_excel("work/hrani.xlsx", skip = 18) %>% slice(-c(549:554)) %>% 
  mutate(across(c(1:9, 15), as.character)) %>% mutate(across(10:14, as.numeric))
hrani_xml <- read_csv("work/hrani.csv") %>%
  select(!starts_with("Partners")) %>% 
  select(1:9, DurationInMonths, Description, Status, everything())
hrani_df <- left_join(hrani, hrani_xml, 
                                 by = c("Номер на проектно предложение" = "Id")) %>%
  pivot_longer(27:180, names_to = "payment", values_to = "value_paid") %>% 
  select(-DateofDecisionforFunding, 
         -`ProjectBeneficiary/EntityId`, 
         -`BeneficiaryFunding/_xsi:nil`)

imsp <- read_excel("work/imsp.xlsx", skip = 18) %>% slice(-c(2:7)) %>% 
  mutate(across(c(1:9, 15), as.character)) %>% mutate(across(10:14, as.numeric))

pmdr <- read_excel("work/pmdr.xlsx", skip = 18) %>% slice(-c(1144:1149)) %>% 
  mutate(across(c(1:9, 15), as.character)) %>% mutate(across(10:14, as.numeric))
pmdr_xml <- read_csv("work/pmdr.csv") %>%
  select(!starts_with("Partners")) %>% 
  select(1:9, DurationInMonths, Description, Status, everything())
pmdr_df <- left_join(pmdr, pmdr_xml, 
                      by = c("Номер на проектно предложение" = "Id")) %>%
  pivot_longer(27:47, names_to = "payment", values_to = "value_paid") %>% 
  select(-DateofDecisionforFunding, 
         -`ProjectBeneficiary/EntityId`, 
         -`BeneficiaryFunding/_xsi:nil`)

npvu <- read_excel("work/npvu.xlsx", skip = 18) %>% slice(-c(10963:10968)) %>% 
  mutate(across(c(1:9, 15), as.character)) %>% mutate(across(10:14, as.numeric))
npvu_xml <- read_csv("work/npvu.csv") %>%
  select(!starts_with("Partners")) %>% 
  select(1:9, DurationInMonths, Description, Status, everything())
npvu_df <- left_join(npvu, npvu_xml, 
                     by = c("Номер на проектно предложение" = "Id")) %>%
  pivot_longer(27:362, names_to = "payment", values_to = "value_paid") %>% 
  select(-DateofDecisionforFunding, 
         -`ProjectBeneficiary/EntityId`, 
         -nil)

r4r_2021_2027 <- read_excel("work/r4r_2021_2027.xlsx", skip = 18) %>% slice(-c(1756:1761)) %>% 
  mutate(across(c(1:9, 15), as.character)) %>% mutate(across(10:14, as.numeric))
r4r_2021_2027_xml <- read_csv("work/r4r_2021_2027.csv") %>%
  select(!starts_with("Partners")) %>% 
  select(1:9, DurationInMonths, Description, Status, everything())
r4r_2021_2027_df <- left_join(r4r_2021_2027, r4r_2021_2027_xml, 
                     by = c("Номер на проектно предложение" = "Id")) %>%
  pivot_longer(27:59, names_to = "payment", values_to = "value_paid") %>% 
  select(-DateofDecisionforFunding, 
         -`ProjectBeneficiary/EntityId`, 
         -nil)

po_2021_2027 <- read_excel("work/po_2021_2027.xlsx", skip = 18) %>% slice(-c(95:100)) %>% 
  mutate(across(c(1:9, 15), as.character)) %>% mutate(across(10:14, as.numeric))
po_2021_2027_xml <- read_csv("work/po_2021_2027.csv") %>%
  select(!starts_with("Partners")) %>% 
  select(1:9, DurationInMonths, Description, Status, everything())
po_2021_2027_df <- left_join(po_2021_2027, po_2021_2027_xml, 
                              by = c("Номер на проектно предложение" = "Id")) %>%
  pivot_longer(27:53, names_to = "payment", values_to = "value_paid") %>% 
  select(-DateofDecisionforFunding, 
         -`ProjectBeneficiary/EntityId`, 
         -`BeneficiaryFunding/_xsi:nil`)

phomp_2021_2027 <- read_excel("work/phomp_2021_2027.xlsx", skip = 18) %>% slice(-c(251:256)) %>% 
  mutate(across(c(1:9, 15), as.character)) %>% mutate(across(10:14, as.numeric))
phomp_2021_2027_xml <- read_csv("work/phomp_2021_2027.csv") %>%
  select(!starts_with("Partners")) %>% 
  select(1:9, DurationInMonths, Description, Status, everything())
phomp_2021_2027_df <- left_join(phomp_2021_2027, phomp_2021_2027_xml, 
                             by = c("Номер на проектно предложение" = "Id")) %>%
  pivot_longer(27:46, names_to = "payment", values_to = "value_paid") %>% 
  select(-DateofDecisionforFunding, 
         -`ProjectBeneficiary/EntityId`, 
         -`BeneficiaryFunding/_xsi:nil`)

pos_2021_2027 <- read_excel("work/pos_2021_2027.xlsx", skip = 18) %>% slice(-c(105:110)) %>% 
  mutate(across(c(1:9, 15), as.character)) %>% mutate(across(10:14, as.numeric))
pos_2021_2027_xml <- read_csv("work/pos_2021_2027.csv") %>%
  select(!starts_with("Partners")) %>% 
  select(1:9, DurationInMonths, Description, Status, everything())
pos_2021_2027_df <- left_join(pos_2021_2027, pos_2021_2027_xml, 
                                by = c("Номер на проектно предложение" = "Id")) %>%
  pivot_longer(27:50, names_to = "payment", values_to = "value_paid") %>% 
  select(-DateofDecisionforFunding, 
         -`ProjectBeneficiary/EntityId`, 
         -`BeneficiaryFunding/_xsi:nil`)

ptp_2021_2027 <- read_excel("work/ptp_2021_2027.xlsx", skip = 18) %>% slice(-c(39:44)) %>% 
  mutate(across(c(1:9, 15), as.character)) %>% mutate(across(10:14, as.numeric))
ptp_2021_2027_xml <- read_csv("work/tehnicheska_pomost.csv") %>%
  select(!starts_with("Partners")) %>% 
  select(1:9, DurationInMonths, Description, Status, everything())
ptp_2021_2027_df <- left_join(ptp_2021_2027, ptp_2021_2027_xml, 
                              by = c("Номер на проектно предложение" = "Id")) %>%
  pivot_longer(27:35, names_to = "payment", values_to = "value_paid") %>% 
  select(-DateofDecisionforFunding, 
         -`ProjectBeneficiary/EntityId`, 
         -`BeneficiaryFunding/_xsi:nil`)

pkip_2021_2027 <- read_excel("work/pkip_2021_2027.xlsx", skip = 18) %>% slice(-c(1622:1627)) %>% 
  mutate(across(c(1:9, 15), as.character)) %>% mutate(across(10:14, as.numeric))
pkip_2021_2027_xml <- read_csv("work/konkurentnosposobnost_i_inovacii_v_predpriqtiqta_2021_2027.csv") %>%
  select(!starts_with("Partners")) %>% 
  select(1:9, DurationInMonths, Description, Status, everything())
pkip_2021_2027_df <- left_join(pkip_2021_2027, pkip_2021_2027_xml, 
                              by = c("Номер на проектно предложение" = "Id")) %>%
  pivot_longer(27:52, names_to = "payment", values_to = "value_paid") %>% 
  select(-DateofDecisionforFunding, 
         -`ProjectBeneficiary/EntityId`, 
         -`BeneficiaryFunding/_xsi:nil`)

pts_2021_2027 <- read_excel("work/pts_2021_2027.xlsx", skip = 18) %>% slice(-c(22:27)) %>% 
  mutate(across(c(1:9, 15), as.character)) %>% mutate(across(10:14, as.numeric))
pts_2021_2027_xml <- read_csv("work/transportna_svarzanost_2021_2027.csv") %>%
  select(!starts_with("Partners")) %>% 
  select(1:9, DurationInMonths, Description, Status, everything())
pts_2021_2027_df <- left_join(pts_2021_2027, pts_2021_2027_xml, 
                               by = c("Номер на проектно предложение" = "Id")) %>%
  pivot_longer(27:30, names_to = "payment", values_to = "value_paid") %>% 
  select(-DateofDecisionforFunding, 
         -`ProjectBeneficiary/EntityId`, 
         -`BeneficiaryFunding/_xsi:nil`)

ubejishte <- read_excel("work/ubejishte.xlsx", skip = 18) %>% slice(-c(26:31)) %>% 
  mutate(across(c(1:9, 15), as.character)) %>% mutate(across(10:14, as.numeric))
ubejishte_xml <- read_csv("work/ubejishte_migraciq_integraciq.csv") %>%
  select(!starts_with("Partners")) %>% 
  select(1:9, DurationInMonths, Description, Status, everything())
ubejishte_df <- left_join(ubejishte, ubejishte_xml, 
                              by = c("Номер на проектно предложение" = "Id")) %>%
  pivot_longer(27:32, names_to = "payment", values_to = "value_paid") %>% 
  select(-DateofDecisionforFunding, 
         -`ProjectBeneficiary/EntityId`, 
         -`BeneficiaryFunding/_xsi:nil`)

ПРБИУГВП <- read_excel("work/ПРБИУГВП.xlsx", skip = 18) %>% slice(-c(47:52)) %>% 
  mutate(across(c(1:9, 15), as.character)) %>% mutate(across(10:14, as.numeric))
ПРБИУГВП_xml <- read_csv("work/ПРБИУГВП.csv") %>%
  select(!starts_with("Partners")) %>% 
  select(1:9, DurationInMonths, Description, Status, everything())
ПРБИУГВП_df <- left_join(ПРБИУГВП, ПРБИУГВП_xml, 
                          by = c("Номер на проектно предложение" = "Id")) %>%
  pivot_longer(27:35, names_to = "payment", values_to = "value_paid") %>% 
  select(-DateofDecisionforFunding, 
         -`ProjectBeneficiary/EntityId`, 
         -`BeneficiaryFunding/_xsi:nil`)

ПРБФВС <- read_excel("work/ПРБФВС.xlsx", skip = 18) %>% slice(-c(27:32)) %>% 
  mutate(across(c(1:9, 15), as.character)) %>% mutate(across(10:14, as.numeric))
ПРБФВС_xml <- read_csv("work/vatreshna_sigurnost.csv") %>%
  select(!starts_with("Partners")) %>% 
  select(1:9, DurationInMonths, Description, Status, everything())
ПРБФВС_df <- left_join(ПРБФВС, ПРБФВС_xml, 
                         by = c("Номер на проектно предложение" = "Id")) %>%
  pivot_longer(27:33, names_to = "payment", values_to = "value_paid") %>% 
  select(-DateofDecisionforFunding, 
         -`ProjectBeneficiary/EntityId`, 
         -`BeneficiaryFunding/_xsi:nil`)

ПНИИДИТ <- read_excel("work/ПНИИДИТ.xlsx", skip = 18) %>% slice(-c(45:50)) %>% 
  mutate(across(c(1:9, 15), as.character)) %>% mutate(across(10:14, as.numeric))
ПНИИДИТ_xml <- read_csv("work/nauchni_izsledvaniq.csv") %>%
  select(!starts_with("Partners")) %>% 
  select(1:9, DurationInMonths, Description, Status, everything())
ПНИИДИТ_df <- left_join(ПНИИДИТ, ПНИИДИТ_xml, 
                       by = c("Номер на проектно предложение" = "Id")) %>%
  pivot_longer(27:50, names_to = "payment", values_to = "value_paid") %>% 
  select(-DateofDecisionforFunding, 
         -`ProjectBeneficiary/EntityId`, 
         -`BeneficiaryFunding/_xsi:nil`)

ppr_2027 <- read_excel("work/ppr_2027.xlsx", skip = 18) %>% slice(-c(128:133)) %>% 
  mutate(across(c(1:9, 15), as.character)) %>% mutate(across(10:14, as.numeric))
ppr_2027_xml <- read_csv("work/razvitie_na_regionite_2021_2027.csv") %>%
  select(!starts_with("Partners")) %>% 
  select(1:9, DurationInMonths, Description, Status, everything())
ppr_2027_df <- left_join(ppr_2027, ppr_2027_xml, 
                        by = c("Номер на проектно предложение" = "Id")) %>%
  pivot_longer(27:32, names_to = "payment", values_to = "value_paid") %>% 
  select(-DateofDecisionforFunding, 
         -`ProjectBeneficiary/EntityId`, 
         -`BeneficiaryFunding/_xsi:nil`)

pmdra_2027 <- read_excel("work/pmdra_2027.xlsx", skip = 18) %>% slice(-c(60:65)) %>% 
  mutate(across(c(1:9, 15), as.character)) %>% mutate(across(10:14, as.numeric))
pmdra_2027_xml <- read_csv("work/morsko_delo_ribarstvo_i_akvakulturi_2021_2027.csv") %>%
  select(!starts_with("Partners")) %>% 
  select(1:9, DurationInMonths, Description, Status, everything())
pmdra_2027_df <- left_join(pmdra_2027, pmdra_2027_xml, 
                         by = c("Номер на проектно предложение" = "Id")) %>%
  pivot_longer(27:28, names_to = "payment", values_to = "value_paid") %>% 
  select(-DateofDecisionforFunding, 
         -`ProjectBeneficiary/EntityId`, 
         -`BeneficiaryFunding/_xsi:nil`)

brexit <- read_excel("work/rezerv.xlsx", skip = 18) %>% slice(-c(4:9)) %>% 
  mutate(across(c(1:9, 15), as.character)) %>% mutate(across(10:14, as.numeric))
brexit_xml <- read_csv("work/brexit.csv") %>%
  select(!starts_with("Partners")) %>% 
  select(1:9, DurationInMonths, Description, Status, everything())
brexit_df <- left_join(brexit, brexit_xml, 
                           by = c("Номер на проектно предложение" = "Id")) %>%
  pivot_longer(27:30, names_to = "payment", values_to = "value_paid") %>% 
  select(-DateofDecisionforFunding, 
         -`ProjectBeneficiary/EntityId`, 
         -`BeneficiaryFunding/_xsi:nil`)

prsr <- read_excel("work/prsr.xlsx", skip = 18) %>% slice(-c(11705:11710)) %>% 
  mutate(across(c(1:9, 15), as.character)) %>% mutate(across(10:14, as.numeric))


programi <- bind_rows(transport_df, okolna_sreda_df, regioni_v_rastej_df, inovacii_df, nauka_df, 
                      r4r_df, dobro_upravlenie_df, hrani_df, imsp, pmdr_df, npvu_df, 
                      r4r_2021_2027_df, po_2021_2027_df, phomp_2021_2027_df,
                      pos_2021_2027_df, ptp_2021_2027_df, pkip_2021_2027_df, pts_2021_2027_df, 
                      ubejishte_df, ПРБИУГВП_df, ПРБФВС_df, ПНИИДИТ_df, ppr_2027_df, pmdra_2027_df, brexit_df, prsr) %>% 
  mutate(ЕИК = str_extract(Бенефициент, "^\\d+"), .before = Бенефициент) %>% 
  mutate(Бенефициент = str_remove(Бенефициент, "^\\d+"),
         Бенефициент = str_squish(Бенефициент)) %>% distinct()
