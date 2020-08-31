
sme <- run_query("Select *, floor(cp_actual_eval*10) as nf, floor(ap_actual_eval*10) as ap_bucket, week(calltime) as 'week'
                  from  `smexplorerdata`.`bskybinternal.smetable_ptf_new`
                  where calltime >= '2020-06-01'", "ai")


rpc_per_nf <- sme %>%
  select(week, nf, issavedtv) %>% 
  group_by(week, nf) %>%
  summarise(cr = mean(issavedtv)) %>%
  setorder(-cr)


rpc_per_nf_qu <- sme %>%
  select(week, nf, issavedtv, ap_actual_eval) %>%
  mutate(AP_quintile = floor(10*ap_actual_eval)) %>%
  group_by(week, AP_quintile, nf) %>%
  filter(!is.na(ap_actual_eval)) %>%
  summarise(cr_qu = mean(issavedtv), calls = n())%>%
  setorder(-cr_qu)

j <- merge(rpc_per_nf, rpc_per_nf_qu, by = c('week', 'nf'))

rpc_per_nf_norm <- j %>%
  filter(cr != 0, calls > 10 )%>%
  mutate(WCR = (cr_qu/abs(cr)) * calls)

rpc_norm <- rpc_per_nf_norm %>%
  group_by(week,AP_quintile)%>%
  summarise(CR_norm = sum(WCR) * 1.0 / sum(calls), calls_normalised = sum(calls))

write.csv(rpc_norm)
