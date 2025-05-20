library(arrow)
library(dplyr)
library(stringr)
library(MatchIt)
library(lubridate)
library(cobalt)
library(ggplot2)
library(survey)
library(broom)

###Fonction d'automatisation

#Extraction d'une certaine base trimestrielle aprÃ¨s 2020
extraction <- function(annee, trimestre){
  return(open_dataset(paste0("//casd.fr/casdfs/Projets/ENSAE05/Data/EE_EEC_20",annee,"/t", annee, trimestre, "z.parquet")) %>% 
           collect()) 
}
#Extraction d'une certaine base trimestrielle avant 2021
extraction_avt <- function(annee, trimestre){
  return(open_dataset(paste0("C:/Users/Public/Documents/Lamothe Perrin/Base/t", annee, trimestre, "z.parquet")) %>% 
           collect()) 
}

#Correction des variables utilisÃ©es pour le PSM
traitement <- function(df){
  df <- df %>% 
    filter(AGE >= 15 & AGE <= 64) %>% 
    mutate(id = paste(IDENT, NOI)) %>% 
    mutate(FNF = case_when(
      FNFPROA_Y == 1 ~ 1,
      TRUE ~ 0
    )) %>% 
    mutate(secteur = case_when(
      !is.na(NAFG021N) ~ NAFG021N,
      !is.na(NAFANTG021N) ~ NAFANTG021N,
      TRUE ~ "Non connue"
    )) %>% 
    mutate(statut_emploi = case_when(
      !is.na(ACTEU) ~ ACTEU,
      TRUE ~ "3"
    )) %>% 
    # mutate(classe_emploi = case_when(
    #   !is.na(ACL_EMPLOI) ~ ACL_EMPLOI,
    #   TRUE ~ "99"
    # )) %>% 
    mutate(age = as.character(AGE)) %>% 
    filter((FNF== 1 & statut_emploi == "1" & STATUT == "2") | (RGA == "1" & statut_emploi == "1" & STATUT == "2"))
  
  psm_df <- df %>% 
    select(id, FNF, AGE, SEXE, DIP7, DEPARTEMENT, secteur, ANNEE, TRIM, RGA, STATUT, SALRED_Y, EXTRI, EXTRID)
  
  return(psm_df)
}

#PSM et df matchÃ©
psm <- function(df){
  df <- df %>% 
    mutate(across(where(is.character), as.factor))
  
  psm_model <- matchit(FNF ~ AGE + SEXE + DIP7 + DEPARTEMENT + secteur,
                       data = df, 
                       method = "nearest",
                       distance = "logit",
                       weights = ~EXTRID)
  
  matched_df <- match.data(psm_model)
  return(list(psm_model, matched_df))
}


###Base matchÃ© pour l'ensemble des individus en formation entre 2021 et 2023 

for(y in c("21","22", "23")){
  for(i in c("1", "2", "3", "4")){
    if(i == "1" & y == "21"){
      psm_all <- psm(traitement(extraction(y, i)))
      psm_mod <- list(psm_all[[1]])
      df_matched <- psm_all[[2]] %>% mutate(subclass = as.character(subclass))
    }else{
      sortie_temp <- psm(traitement(extraction(y, i)))
      psm_mod <- c(psm_mod, list(sortie_temp[[1]]))
      df_temp <- sortie_temp[[2]] %>% 
        mutate(subclass = as.character(subclass))
      df_matched <- rbind(df_matched, df_temp)
    }
  }
} 

#Statistiques déscriptives sur les personnes ayant déclaré avoir suivi une formation non formelle
df_FNF <- df_matched %>% filter(FNF == 1) %>% 
  mutate(tranche_age = cut(AGE,
                           breaks = c(15,25,35,45,55,65),
                           right = FALSE,
                           labels = c("15-24", "25-34", "35-44", "45-54", "55-64")))
design_treated <- svydesign(ids= ~1, data = df_FNF, weights = ~EXTRID)

prop_DIP <- svymean(~DIP7, design = design_treated)
prop_AGE <- svymean(~AGE, design = design_treated) 
prop_SEC <- svymean(~secteur, design = design_treated) 

prop_age <- svymean(~tranche_age, design = design_treated) 

df_DIP <- as.data.frame(prop_DIP) %>% 
  mutate(Proportion = mean * 100,
         `IC95%_bas` = (mean - 1.96 * SE) * 100,
         `IC95%_haut` = (mean + 1.96 * SE) * 100,
         ) %>% 
  select(Proportion, `IC95%_bas`, `IC95%_haut`)

df_AGE <- as.data.frame(prop_AGE) %>% 
  mutate(Moyenne = mean,
         `IC95%_bas` = (mean - 1.96 * AGE),
         `IC95%_haut` = (mean + 1.96 * AGE),
         ) %>% 
  select(Moyenne, `IC95%_bas`, `IC95%_haut`)

df_age <- as.data.frame(prop_age) %>% 
  mutate(Proportion = mean * 100,
         `IC95%_bas` = (mean - 1.96 * SE) * 100,
         `IC95%_haut` = (mean + 1.96 * SE) * 100,
  ) %>% 
  select(Proportion, `IC95%_bas`, `IC95%_haut`)

df_SEC <- as.data.frame(prop_SEC) %>% 
  mutate(Proportion = mean * 100,
         `IC95%_bas` = (mean - 1.96 * SE) * 100,
         `IC95%_haut` = (mean + 1.96 * SE) * 100,
  ) %>% 
  select(Proportion, `IC95%_bas`, `IC95%_haut`)


#On recupere les informations de salaire pour les personnes rentrees aprÃ¨s 2020
for(y in c("21","22", "23")){
  for(i in c("1", "2", "3", "4")){
    if(i == "1" & y == "21"){
      infopdt <- extraction(y, i) %>% 
        filter(AGE >= 15 & AGE <= 64) %>% 
        mutate(id = paste(IDENT, NOI)) %>% 
        select(id, AGE, RGA, SEXE, SALRED_Y, ANNEE, TRIM, STATUT, EXTRID) %>% 
        filter((RGA == "1") & (id %in% df_matched$id)) 
    }else{
      sortie_temp <- extraction(y, i) %>% 
        filter(AGE >= 15 & AGE <= 64) %>% 
        mutate(id = paste(IDENT, NOI)) %>% 
        select(id, AGE, RGA, SEXE, SALRED_Y, ANNEE, TRIM, STATUT, EXTRID) %>% 
        filter((RGA == "1") & id %in% df_matched$id)
      infopdt <- rbind(infopdt, sortie_temp)}
  }
} 

#On recupere les informations precedentes pour recuperer le salaire des personnes rentrees avant 2021
for(y in c("19","20")){
  for(i in c("1", "2", "3", "4")){
    if(i == "1" & y == "19"){
      infoavant <- extraction_avt(y, i) %>% 
        mutate(AGE = as.numeric(AGE)) %>% 
        filter(AGE >= 15 & AGE <= 64) %>% 
        mutate(id = paste(IDENT, NOI)) %>% 
        select(id, AGE, RGA, SEXE, SALRED, ANNEE, TRIM, STAT2) %>% 
        filter((RGA == "1") & (id %in% df_matched$id)) 
    }else{
      sortie_temp <- extraction_avt(y, i) %>% 
        mutate(AGE = as.numeric(AGE)) %>% 
        filter(AGE >= 15 & AGE <= 64) %>% 
        mutate(id = paste(IDENT, NOI)) %>% 
        select(id, AGE, RGA, SEXE, SALRED, ANNEE, TRIM, STAT2) %>% 
        filter((RGA == "1") & id %in% df_matched$id)
      infoavant <- rbind(infoavant, sortie_temp)}
  }
} 


infopdt <- infopdt %>% 
  select(id, SALRED_Y, RGA) %>%
  filter(RGA == "1") %>% 
  select(-RGA)

infoavant <- infoavant %>% 
  select(id, SALRED, RGA) %>% 
  filter(RGA == "1") %>% 
  select(-RGA)

#On corrige la variable salaire avec les informations de première vague pour les personnes ayant suivi une formation
df_matched_sal <- df_matched %>% 
  left_join(infoavant, by = "id") %>% 
  left_join(infopdt, by = "id") %>% 
  mutate(SALRED_nv = case_when(
    !is.na(SALRED_Y.x) ~ SALRED_Y.x,
    TRUE ~ 0)) %>% 
  mutate(SALRED_nv_1 = case_when(
    !is.na(SALRED) ~ as.numeric(SALRED),
    TRUE ~ 0)) %>% 
  mutate(SALRED_nv_2 = case_when(
    !is.na(SALRED_Y.y) ~ SALRED_Y.y,
    TRUE ~ 0)) %>% 
  mutate(SALRED_nv_3 = case_when(
    SALRED_nv > 0 ~ SALRED_nv,
    SALRED_nv_2 > 0 ~ SALRED_nv_2,
    TRUE ~ SALRED_nv_1
  )) %>% select(-SALRED_Y.x, -SALRED_Y.y, -SALRED, -SALRED_nv, -SALRED_nv_1, - SALRED_nv_2)


#Graphiques des rÃ©sultats du matching pour vÃ©rifier l'efficacitÃ© du matching 
for (i in seq(1,12)){
  if(i == 1){
    df <- cbind(distance = psm_mod[[1]]$distance, treat = psm_mod[[1]]$treat, weights = psm_mod[[1]]$weights, psm_mod[[1]]$X) %>% 
      mutate(group = case_when(
        treat == 1 ~ 'Treated',
        weights == 1 & treat == 0 ~ 'Control matched',
        TRUE ~ 'Control unmatched'
      ))
  }else{
    df_temp <- cbind(distance = psm_mod[[i]]$distance, treat = psm_mod[[i]]$treat, weights = psm_mod[[i]]$weights, psm_mod[[i]]$X) %>% 
      mutate(group = case_when(
        treat == 1 ~ 'Treated',
        weights == 1 & treat == 0 ~ 'Control matched',
        TRUE ~ 'Control unmatched'
      ))
    df <- rbind(df, df_temp)
  }
}



df <- cbind(distance = psm_mod[[1]]$distance, treat = psm_mod[[1]]$treat, weights = psm_mod[[1]]$weights, psm_mod[[1]]$X) %>% 
  mutate(group = case_when(
    treat == 1 ~ 'Treated',
    weights == 1 & treat == 0 ~ 'Control matched',
    TRUE ~ 'Control unmatched'
  ))
ggplot(df, aes(x = distance, fill = group)) +
  geom_density(alpha = 0.9) +
  scale_fill_manual(values = c('red', 'blue', 'black'), labels = c('Contrôle matché', 'Contrôle avant matching','Traité')) +
  theme_minimal()


#On fait une première regression avec les pondérations de l'enquête pour estimerla différence de salaire antérieur à la formation 
df_matched_sal <- df_matched_sal %>% 
  mutate(poids_corriges = weights * EXTRID)

result <- lm(SALRED_nv_3  ~ FNF, data = df_matched_sal, weights = poids_corriges)
summary(result)

#On fait quelques graphiques pour observer les différences de salaires entre le groupe de contrôle et le groupe traité


design <- svydesign(ids = ~1, data = df_matched_sal, weights = ~poids_corriges)

moyennes <- svyby(~SALRED_nv_3, ~FNF, design = design, svymean, na.rm = TRUE)
moyennes <- as.data.frame(moyennes)

ggplot(moyennes, aes(x = factor(FNF, labels = c("Controle", "Traité")), y = SALRED_nv_3)) +
  geom_col(fill = "skyblue", width = 0.6) +
  geom_errorbar(aes(ymin = SALRED_nv_3 - se, ymax = SALRED_nv_3 + se), width = 0.2) +
  labs(title = "Effet de la formation sur le salaire précédent",
       x = "Groupe",
       y = "Valeur moyenne pondéré du Salaire") + 
  theme_minimal()

ggplot(df_matched_sal, aes(x=AGE, y = SALRED_nv_3, color = factor(FNF))) +
  # geom_point(alpha = 0.3, aes(size = poids_corriges), shape = 1) +
  geom_smooth(method = "loess", se = TRUE)+
  scale_color_manual(values = c("blue", "red"),
                     labels = c("Contrôle", "Traité")) +
  labs(
       x = "Âge",
       y = "Salaire",
       color = "Groupe") +
  theme_minimal()

ggplot(df_matched_sal %>% filter(SALRED_nv_3<10000), aes(x= SALRED_nv_3, fill = factor(FNF), weight = poids_corriges)) +
  geom_density(alpha = 0.4) +
  scale_fill_manual(values = c("grey70", "tomato"),
                    labels = c("Contrôle", "Traité")) +
  labs(title = "Distribution du salaire au moment de l'enquête selon la participation à une formation", 
       x = "Salaire",
       y = "Densité estimée",
       fill = "Groupe") +
  theme_minimal()

#On fait une régression par quantile de salaire pour voir si les effets dépendent 
library(quantreg)

quantiles <- seq(0.1,0.9, by = 0.1)

rq_list <- lapply(quantiles, function(tau){
  rq(SALRED_nv_3 ~ FNF, data = df_matched_sal, weights = poids_corriges, tau = tau)
})

results <- bind_rows(
  lapply(seq_along(quantiles), function(i){
    coef_summary <- summary(rq_list[[i]], se = "nid")
    tibble(
      tau = quantiles[i],
      estimate = coef_summary$coefficients["FNF", "Value"],
      std_error = coef_summary$coefficients["FNF", "Std. Error"],
      p_value = coef_summary$coefficients["FNF", "Pr(>|t|)"]
    )
  })
)

results <- results %>% 
  mutate(ci_lower = estimate - 1.96 * std_error,
         ci_upper = estimate + 1.96 * std_error)

ggplot(results, aes(x = tau, y = estimate)) +
  geom_line(color = "tomato", size = 1.2) +
  geom_ribbon(aes(ymin = ci_lower, ymax = ci_upper), alpha = 0.2, fill = "tomato") +
  geom_hline(yintercept = 0, linetype = "dashed", color = "grey40") +
  labs(
       x = " Quantile du salaire",
       y = "Ecart salarial au sein du quantile",
       caption = "Régression quantile pondéré") +
  theme_minimal()




