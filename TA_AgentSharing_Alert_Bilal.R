library(easy.db.query)
library(data.table)
library(stats)
library(RMySQL)
library(janitor)
library(dplyr)
library(plyr)
library(stringr)
library(tidyr)


# Fetching Evaluatorlog for Last 10 Days for All Areas
Eval_All  <-  run_query("select date(calltime) as date, agentid, area, calltime, skill 
                        from `bskybinternal.vevaluatorlog`
                        where calltime > date_sub(curdate(), interval 11 day) and calltime < curdate();", "arch") 

setDT(Eval_All)

Eval_TA <- Eval_All[!is.na(agentid) & area == "Platform Classic"] # Evaluatorlog for TA

############################# Creating Mapping of Each Agent who took Calls on TA #####################
# It will pick all agents who took calls on TA in Last 10 Days and then see on which other skills they
# have taken calls on and then based on those skills decide which Area does those agents belong to

Agents_PTFCalls <- Eval_TA[, unique(agentid)] # Agents that took calls on PTF
Agents_AllSkills <- Eval_All[agentid %in% Agents_PTFCalls, .(all_skills = toString(sort(unique(skill)))), by = .(date, agentid)]
Agents_All_Skills_Area  <- Agents_AllSkills[, Area := case_when(
  all_skills %like% "ATT" ~ "Attractions",
  all_skills %like% "ATH" ~ "Upgrades", 
  all_skills %like% "FSM" ~ "Upgrades",
  all_skills %like% "TSF" ~ "Mobile", 
  all_skills %like% "SER" ~ "Service",
  all_skills %like% "SMPRET_VAL" ~ "Value",
  all_skills %like% "SMPRET_PTF" ~ "Platform",
  TRUE ~ "OtherAreas")]

Eval_TA_Area <- Agents_All_Skills_Area[Eval_TA[,.(date, agentid)], on = .(date,agentid)]

################################### Checking Call Counts and Agent Counts of Other Areas who took Platform Calls #######

df <- Eval_TA_Area[,.(Calls = .N, Agent_Count = length(unique(agentid))), by = .(date, Area)]

callcount_dcast <- dcast(df, date~Area, value.var = "Calls")
agentcount_dcast <- dcast(df, date~Area, value.var = "Agent_Count")
callcount_dcast[is.na(callcount_dcast)] <- 0
agentcount_dcast[is.na(agentcount_dcast)] <- 0

Distinct_Areas <- c("Platform", "Value", "Service", "Upgrades", "Attractions", "Mobile", "OtherAreas")
Areas_Missing_From_Data <- setdiff(Distinct_Areas, colnames(callcount_dcast))
callcount_dcast[ ,Areas_Missing_From_Data] <- 0
agentcount_dcast[ ,Areas_Missing_From_Data] <- 0

callcount_pivot <- callcount_dcast %>% adorn_percentages("row") %>% adorn_pct_formatting() %>% adorn_ns() 
agentcount_pivot <- agentcount_dcast %>% adorn_percentages("row") %>% adorn_pct_formatting() %>% adorn_ns()
callcount_pivot <- callcount_pivot %>% mutate_all(as.character)
agentcount_pivot <- agentcount_pivot %>% mutate_all(as.character)

run_query("Delete from `aidev`.`agent_sharing_ptf_agentcounts`", 'arch_aidev')
con<- dbConnect(dbDriver("MySQL"),user="satmap_ai_bskyb", password="$bskyb#123", dbname="aidev", host="10.105.2.57", port = 3307);
dbWriteTable(con, name = "agent_sharing_ptf_agentcounts", value = agentcount_pivot , overwrite = F, append = T, row.names = F)


run_query("Delete from `aidev`.`agent_sharing_ptf_callcounts`", 'arch_aidev')
con<- dbConnect(dbDriver("MySQL"),user="satmap_ai_bskyb", password="$bskyb#123", dbname="aidev", host="10.105.2.57", port = 3307);
dbWriteTable(con, name = "agent_sharing_ptf_callcounts", value = callcount_pivot , overwrite = F, append = T, row.names = F)






