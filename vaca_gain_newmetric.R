sme <- ("select callstarttime as calltime, on_off, revenue, revenue_trueup, issavedtv, issavedtv_trueup, case when issavedtv = 1 then revenue end revenue_save,
case when issavedtv_trueup = 1 then revenue_trueup end revenue_save_trueup, precall_revenue
         from `bskybsme`.`vaca_ta`
         where callstarttime > '2020-08-27 00:00:00'
         and callstarttime < '2020-08-27 14:00:00'
         and customgroup = 'Platform Classic'
         and isrelevant = 1
         ")
sme <- run_query(sme, "arch_bskybsme")
setDT(sme)
sme[, date := as.Date(calltime)]
sme.gain <- melt(sme, id.vars = c("date", "on_off"), measure.vars = 3:9, variable.name = "opt")


#View(sme)
### Gain Plots {.tabset .tabset-fade}

#### Gain Correlations

sme.gain <- melt(sme, id.vars = c("date", "on_off"), measure.vars = 3:9, variable.name = "opt")
sme.calls <- sme[,.(calls_ON = sum(as.numeric(on_off)), calls_OFF = sum(1-as.numeric(on_off))), by = date]
sme1 <- sme.gain[,.(avg = mean(value, na.rm = T)), by = .(date, on_off = ifelse(on_off == 1, "ON", "OFF"), opt)]
sme2 <- dcast(sme1, date~on_off+opt, value.var = "avg")
sme2 <- merge(sme2, sme.calls, by = "date")
sme2[, `:=`(
  revenue_gain = ON_revenue - OFF_revenue,
  revenue_trueup_gain = ON_revenue_trueup - OFF_revenue_trueup,
  issavedtv_gain = ON_issavedtv - OFF_issavedtv,
  issavedtv_trueup_gain = ON_issavedtv_trueup - OFF_issavedtv_trueup,
  revenue_save_gain = ON_revenue_save - OFF_revenue_save,
  revenue_save_trueup_gain = ON_revenue_save_trueup - OFF_revenue_save_trueup, 
  precall_revenue_gain = ON_precall_revenue - OFF_precall_revenue
)][, `:=`(
  revenue_incrementals = revenue_gain * calls_ON,
  revenue_trueup_incrementals = revenue_trueup_gain * calls_ON,
  issavedtv_incrementals = issavedtv_gain * calls_ON,
  issavedtv_trueup_incrementals = issavedtv_trueup_gain * calls_ON,
  revenue_save_incrementals = revenue_save_gain * calls_ON,
  revenue_save_trueup_incrementals = revenue_save_trueup_gain * calls_ON, 
  precall_revenue_incrementals = precall_revenue_gain * calls_ON
  
)
]


cols <- colnames(sme2)[c(16:21)]
#ggpairs(sme2[,..cols])



#### Gain Table

sme3 <- melt(sme2, id.vars = 1, measure.vars = 25:31, variable.name =  "opt", value = "incrementals")
sme3[, opt:=str_replace(opt, "_incrementals","")]
sme3 <- merge(sme3, sme.calls, by = "date")

kable(sme3[, .(Min_Date = min(date), Max_Date = max(date), Incrementals = sum(incrementals), ABS_Gain = sum(incrementals)/sum(calls_ON)) , by = opt]) %>% kable_styling()

