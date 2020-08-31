library(easy.db.query)
library(data.table)
library(dplyr)
library(ggplot2)
library(plotly)


model.settings              <- list()

#Here you tell it the queue i.e. upgrades, service, platform or value
model.settings$queue.name   <- "platform"

#In case there are more than one models you tell which model you want to generate the report for 
#i.e. combined, diamond, billing, hm etc. 
model.settings$model.name   <- ""

#Tell the date on which you want to run the variable analysis
model.settings$start.date <- '2020-06-01'
model.settings$end.date <- '2020-08-23'  

#Define set of callgroup variables for which you want to run the analysis
variables = c( "rep_apptag_bskyb","dtv_package", "dtv_tenure", "rev_value_buckets_crm", "segment", 
              "vdn_bskyb", "modelgroups2"
            )

if (model.settings$queue.name == "upgrades" && model.settings$model.name == "combined")
{
  
  sme_table = "`bskybinternal.smetable_upgrades_newdg2`"
  model_filter = "and rep_anscomp_bskyb in ('CMP_SATMAP_SAL_ExistCust_COMBINED','CMP_SATMAP_SAL_ExistCust_Combined_SIVRs','CMP_SATMAP_SAL_UK_SkyQ_ExistCust',
  'CMP_SATMAP_SAL_Hood1_Upgrades','CMP_SATMAP_SAL_Hood2_Upgrades','CMP_SATMAP_SAL_Hood3_Upgrades', 'CMP_SATMAP_SAL_Upgrades')"
  
} else if (model.settings$queue.name == "upgrades" && model.settings$model.name == "diamond"){
  
  sme_table = "`bskybinternal.smetable_upgrades_newdg2`"
  model_filter = "and rep_anscomp_bskyb like ('%diamond%') and isrelevant = 1" 
  
} else if (model.settings$queue.name == "value"){
  
  sme_table = "`bskybinternal.smetable_value_newdg2`"
  model_filter = "and isrelevant = 1" 
  
  
} else if (model.settings$queue.name == "platform"){
  
  sme_table = "`bskybinternal.smetable_ptf_new`"
  model_filter = "and isrelevant = 1"   
  
} else if (model.settings$queue.name == "service"){
  
  
  sme_table <- paste0("`skyservice.smetable_", model.settings$model.name, "_newdg2`")
  model_filter = "and isrelevant = 1" 
}


model.settings$select.query <- paste0("select * from ", sme_table, " where calltime > '", model.settings$start.date, "' and calltime < '",  model.settings$end.date, "'", model_filter)
cg_var.data <- run_query(model.settings$select.query, 'ai')
cg_var.data <- data$training
nrow(cg_var.data)


OtherCGVariables <- function(cg_var.data){
  
  weekly_stats <- list()
  
  for(i in variables){
    
    data.1 <- cg_var.data %>% group_by(week = week(calltime) , var = get(i)) %>%
      dplyr::summarise( min_dt = min(date(calltime)) , max_dt = max(date(calltime)) ,  calls = n() , saves = sum(issale)) %>% group_by(week) %>% mutate(week_period = paste0(format(min(as.Date((min_dt))) , '%b-%d'), "  -  " , format(max(as.Date((max_dt))), '%b-%d'))) %>%
      group_by(var) %>% mutate(cg_calls_perc = 100*sum(calls)/nrow(cg_var.data))
    setDT(data.1)
    
    data.1[cg_calls_perc <= 1 , var := 'smalls']
    data.1 <- data.1[,.(calls = sum(calls) , cr = sum(saves)/sum(calls)) , by = .(week_period , var)]
    
    if("plyr" %in% (.packages())){
      detach("package:plyr", unload=TRUE) 
    }  
    
    data.1 <- data.1  %>% group_by(week_period)  %>% mutate(var_ranks = order(order(-cr)))
    data.1$week_period <- factor(data.1$week_period , unique(data.1$week_period ))
    weekly_stats[[i]] <- data.1
  }
  
  return(weekly_stats)
  
}

#View(other.cgvariables$last_bill_amount)

other.cgvariables <- OtherCGVariables((cg_var.data ))
setwd("~/")
pdf(paste(model.settings$queue.name,"_",model.settings$model.name,"_New_CG_Var_Trends.pdf")   , height = 9, width = 20) 
for( i in variables){
  
  
  a <- ggplot(other.cgvariables[[i]] , aes(x = (week_period) , y = calls , fill = as.factor(var) )) + geom_bar(stat = "identity" , position = "fill" , color = "black")+
    ggtitle(paste0(i , " Week Wise Call Volume")) + theme(axis.text.x = element_text(angle = 90)) +
    theme(legend.position="bottom")
  
  
  b <-  ggplot(other.cgvariables[[i]] , aes(x = week_period , y = cr)) +
    geom_line(aes(group = as.factor(var) ,  color = as.factor(var)))+ geom_point()+
    ggtitle(paste0(i , " Week Wise CR")) + theme(axis.text.x = element_text(angle = 90)) + 
    theme(legend.position="bottom")
  
  c <-  ggplot(other.cgvariables[[i]] , aes(x = week_period , y = var_ranks)) +
    geom_line(aes(group = as.factor(var) ,  color = as.factor(var)))+ geom_point()+
    ggtitle(paste0(i , " Week Wise Rank")) + theme(axis.text.x = element_text(angle = 90)) + 
    theme(legend.position="bottom")
  
  
  gridExtra::grid.arrange(a,b,c,ncol = 3)
  
  
}

dev.off()

