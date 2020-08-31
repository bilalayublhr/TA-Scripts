library(data.table)
library(plyr)
library(dplyr)
library(tidyr)
library(easy.db.query)
library(stringr)
library(magrittr)
library(ggplot2) 
library(lubridate)
library(janitor)


end_date <- "2020-07-15"
start_date <- "2020-07-13 09:00:00"



eclext_sql <- paste0("select calltime, callguid, genericlookupstatus from `engine.eclext` 
                     where calltime >= '", start_date, "' and calltime <= '", end_date, "'")

eclext <- run_query(eclext_sql, 'arch')

ptf_ecl <- run_query(paste0("Select callguid from `bskybsatmap`.`bskybinternal.vecl` where 
                            Area = 'Platform Classic' and successful = 1 and
                            calltime >= '", start_date, "' and calltime <= '", end_date, "'"), "arch")


a <- eclext %>% mutate(split = str_split(genericlookupstatus, "\\|")) %>% unnest(split) %>% 
  mutate(lookupnode = str_extract(split, "[a-zA-Z0-9_]+,"), split = str_replace(split, lookupnode, "")) %>%
  mutate(lookuptime = str_replace(str_extract(split, "[a-z],[0-9]+"), "[a-z],","")) %>% 
  mutate(lookupnode = str_replace_all(lookupnode, ",", "")) %>% 
  mutate(lookupstatus = str_replace_all(str_extract(split, ",[A-Za-z]+,"),",", "")) %>% filter(!is.na(genericlookupstatus)) %>% 
  select(callguid, calltime, lookupnode, lookuptime, lookupstatus)

setDT(a)
a$lookuptime <- as.numeric(a$lookuptime)
df <- a[callguid %in% ptf_ecl$callguid, .(Counts = .N), by = .(lookupnode,lookupstatus)]
df_lookuptime <- a[callguid %in% ptf_ecl$callguid, .(Avg_LookupTime = round(mean(as.numeric(lookuptime)),2)), by = .(lookupnode)]
final <- dcast(df, lookupnode ~ lookupstatus, value.var = "Counts")
final[is.na(final)] <- 0
final$False <- as.numeric(final$False)
final$Timeout <- as.numeric(final$Timeout)
final$True <- as.numeric(final$True)
final$LookupKey_NA <- nrow(ptf_ecl) 
final <- final[, LookupKey_NA := LookupKey_NA - False - True - Timeout]
final <- df_lookuptime[final, on = "lookupnode"]
final$Avg_LookupTime <- as.character(final$Avg_LookupTime)
lookup_matrix <- final  %>% adorn_percentages("row") %>% adorn_pct_formatting(digits = 2) %>% adorn_ns(position = "front") 

View(lookup_matrix)
#View(a)

conn <- dbConnect(MySQL(), user = "satmap_ai_bskyb", password = "$bskyb#123", 
                  dbname = "etl_dev", host = "10.105.2.57", 
                  port = 3307)

dbWriteTable(conn, name = "ptf_lookup_status", value = lookup_matrix[c("CRM","DGIQM", "DG3AC", "DG3BTN", "dtv_cancel", "sky_transfer_history", "accounthistory", "DoNotPair")]  , overwrite = T, append = F, row.names = F)


