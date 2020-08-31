library(easy.db.query)
library(data.table)
library(stats)
library(RMySQL)
library(tictoc)

tic()

############################################################################################################

SelectEvent_Minutes <- run_query("select value from bskybsatmap.`bskybinternal.spinputs` where SPName = 'Agent_Loggedin_Repercentile' and variable = 'Event_Minutes';", "prod") %>% as.numeric() # agent log picked for last 60 minutes

ActiveModels_For_Reperc <- run_query("select value from bskybsatmap.`bskybinternal.spinputs` where SPName = 'Agent_Loggedin_Repercentile' and variable = 'active_models_for_ap_reperc';", "prod")
ActiveModels_For_Reperc <- as.list(strsplit(ActiveModels_For_Reperc$value, ",")[[1]])

Calls_Threshold <- run_query("select value from bskybsatmap.`bskybinternal.spinputs` where SPName = 'Agent_Loggedin_Repercentile' and variable = 'calls_threshold';", "prod") %>% as.numeric() # agent log picked for last 60 minutes

############################################################################################################
con<- dbConnect(dbDriver("MySQL"),user="satmap_ai_bskyb", password="MM@$346",
                dbname="bskybsatmap", host="10.105.11.51", port = 3307);

sql = paste0("select skill, agentid, eventtime, EventType 
                      from bskybsatmap.`engine.agentlog`
                      where eventtime <= '", Sys.time(), "' and eventtime >= '", Sys.time() - SelectEvent_Minutes*60,"';")

dbSendQuery(con, 'set session transaction isolation level read uncommitted')
rs <- dbSendQuery(con, sql)
agentlog <- fetch(rs, n=-1)
setDT(agentlog)
colnames(agentlog) <- tolower(colnames(agentlog))
dbClearResult(rs)
lapply( dbListConnections( dbDriver( drv = "MySQL")), dbDisconnect)

####################################################################################################3

split <- paste0("('", paste(ActiveModels_For_Reperc, collapse="\', \'"), "')") #Formating Area so that it can be used in Where Clause in below run_query
Area <-  run_query(paste0("select distinct(Area) from `smexplorerdata`.`agentgroupsmapping` where iscurrent = 1 and model in ", split, ";"), "ai")  
Area <- as.list(strsplit(Area$area, ","))
Area <- paste0("('", paste(Area, collapse="\', \'"), "')") 


######################################Fetching Area wise Calls for current date##############################
calls_model <- run_query(paste0("select Area, count(*) as calls 
                      from `bskybsatmap`.`bskybinternal.vevaluatorlog`
                      where calltime >= '", Sys.Date(),"'
                          and Area in ", Area ," group by 1;"), "arch") 

setDT(calls_model) # set calls_model as Data Table
calls_model <- calls_model[, `:=`(area = gsub( " ", "", calls_model$area))] # Replacing 'Platform Classic' with 'PlatformClassic'


#################################Fetching AP Table with original APs for all models###########################

ap_table_original <- run_query(paste0("select * from bskybsatmap.`bskybinternal_originalap.agentperformance` where iscurrent =1 and split in ", split, ";"),  "prod") %>% setDT()

######################################Fetching Agent Attributes################################################

agentattributes <- run_query("select agentid,param5 as actualagentid,engineid,updatedon
                                    from bskybsatmap.`engine.agentattributes`
                                    order by updatedon desc;", "prod") %>% setDT()


agentattributes[, updatedon_max := max(updatedon) , by = actualagentid]
agentattributes <- agentattributes[updatedon == updatedon_max]
#################################### Repercentiling Function ################################################

reperc_ap_table <- function(ActiveModels_For_Reperc, SelectEvent_Minutes, Calls_Threshold, agentlog, calls_model, ap_table_original, agentattributes){

ap_table_reperc <- list()
    
for (i in ActiveModels_For_Reperc){  
  
    Model <- i  
    calls_i <- calls_model[area %in% i]

    if(length(calls_i$calls)==0){
        calls <- 0 %>% as.numeric()
    }else{
        calls <- calls_i$calls %>% as.numeric()
    }
    
    if (calls <= Calls_Threshold){
  
      ap_table_reperc[[as.character(i)]] <- ap_table_original[split %in% i]
      
    }else{
  
        skill_model <- run_query(paste0("select skill from `smexplorerdata`.`agentgroupsmapping`
                     where iscurrent = 1 and model = '", Model, "';"), "ai") %>% setDT()


        agentlog_model <- agentlog[skill %in%  skill_model$skill]
        agentlog_model_maxeventtime <- agentlog_model[, .(maxeventtime = max(eventtime)), by = .(agentid)]
        agentlog_model_join_maxeventtime <- agentlog_model[agentlog_model_maxeventtime, on = "eventtime==maxeventtime"]
        agentlog_model_join_maxeventtime_1 <- agentlog_model_join_maxeventtime[, .(eventcount = .N, events = toString(sort(unique(eventtype)))), by = agentid] %>% mutate(events = tolower(events)) %>% setDT()
        loggedin_agents <- agentlog_model_join_maxeventtime_1[!(events %like% "logout" | events %like% "unavailable") | events %like% 'login']

      
        loggedin_agents_actualagentid <- agentattributes[loggedin_agents, on = "agentid"] %>%
        select(agentid, eventcount, events, actualagentid)
      
        ap_table_model <- ap_table_original[split == i][, comments := paste0("Original AP: ", percentile, ",")]
      
      
        loggedin_agents_actualagentid_apactual <- loggedin_agents_actualagentid[ap_table_model, on = "actualagentid==agentid", nomatch = 0] %>% dplyr::rename(ap_actual = percentile) %>%
        select(agentid, eventcount, events, actualagentid, ap_actual) %>% setDT()
        loggedin_agents_actualagentid_apactual <- loggedin_agents_actualagentid_apactual[order(ap_actual), `:=`(rank = .I)][, `:=`(AP_loggedin = (rank - 0.5)/.N)]
      
      
        merged <- merge(ap_table_model, loggedin_agents_actualagentid_apactual , by.x = "agentid", by.y = "actualagentid", all.x = T)
        ap_table_model <- merged[!is.na(AP_loggedin), percentile := AP_loggedin] 
        ap_table_model <- ap_table_model[, comments := paste0(comments, "Repercentiled AP: ", round(AP_loggedin,8))] %>%
        select(-eventcount, -events, -ap_actual, -agentid.y, -AP_loggedin, -rank)
        
        ap_table_reperc[[as.character(i)]] <- ap_table_model
  
    }

  }
  ap_table_model_combined <- rbindlist(ap_table_reperc)
  return(ap_table_model_combined)
}


### This will call the repercentile function created above
ap_table_reperc_combined <- reperc_ap_table(ActiveModels_For_Reperc, SelectEvent_Minutes, Calls_Threshold, agentlog, calls_model, ap_table_original,agentattributes) 



### This will Insert the RepercentiledAPs of All Models into a Permanent table for Later Join
run_query("Delete from bskybsatmap.`skyinternal.agentperformance`", 'prod')
con<- dbConnect(dbDriver("MySQL"),user="satmap_ai_bskyb", password="MM@$346", dbname="bskybsatmap", host="10.105.11.51", port = 3307);
dbWriteTable(con, name = "skyinternal.agentperformance", value = ap_table_reperc_combined , overwrite = F, append = T, row.names = F)

#### This will update the main AP Table with the repercentiled AP Table created above on Production Server ####
# run_query("update bskybsatmap.`bskybinternal.agentperformance` iscurrentAP
#           join bskybsatmap.`skyinternal.agentperformance` AP_table
#           on iscurrentAP.agentkey = AP_table.agentkey
#           set iscurrentAP.comments = AP_table.comments , iscurrentAP.percentile = AP_table.percentile
#           where iscurrentAP.iscurrent = 1
#           and iscurrentAP.split = AP_table.split,"prod")
# 


toc()



                      







