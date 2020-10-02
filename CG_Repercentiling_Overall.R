tic() # Starting clock to Calculate Overall Execution Time of SP

ActiveModels_For_Reperc_Original <- run_query("select value from bskybsatmap.`bskybinternal.spinputs` where SPName = 'CG_Repercentile' and variable = 'active_models_for_cg_reperc';", "prod")
ActiveModels_For_Reperc <- tolower(ActiveModels_For_Reperc_Original) 
ActiveModels_For_Reperc <- as.list(strsplit(ActiveModels_For_Reperc, ",")[[1]])
Model <- paste0("('", paste(ActiveModels_For_Reperc, collapse="\', \'"), "')") # Converting Model name to MySQL Where Clause format


# This will fetch Calls Threshold from spinputs. If the Call Volume for any Model is less than Calls Threshold Reprcentling will not occur
Calls_Threshold <- run_query("select value from bskybsatmap.`bskybinternal.spinputs` where SPName = 'CG_Repercentile' and variable = 'calls_threshold';", "prod") %>% as.numeric() 


###################################### Relevant VDNs from VQAREAMAPPING ##############################

relevantvdns <- run_query("select vq_Sct as vdn from bskybsatmap.`bskybinternal.vqareamapping` 
                          where Area in ('BBT', 
                          'Billing',
                          'OtherServices',
                          'TVTech', 
                          'Platform Classic')",
                          "arch") 

################################# Fetching Deployed CGs for all the Active Models ###########################

deployed_cgs <- run_query(paste0("Select *, round((maxpercentile-minpercentile)*100,1) as Expected_BW from `aidev`.`bskybinternal_originalcgs.callgroups` where iscurrent = 1 and callgroup not like '%service_vdn_global%';"),  "arch_aidev") %>% setDT()
deployed_cgs <- deployed_cgs[, model := str_remove_all(str_extract(callgroup,"_[a-zA-Z]+_"),"_")] # Extracting Model Name from Callgroup Name
deployed_cgs$model <- gsub( "platform", "platform classic", deployed_cgs$model) #Subtituing Platform in above Model Column with Platform Classic for consistency

deployed_cgs_relevant <- deployed_cgs[model %in% unlist(ActiveModels_For_Reperc)] #Filtering Deployed Callgroups for only Active Models

deployed_cgs_relevant[, callgroup := str_remove_all(callgroup, "\\|VDNType:.*")] #  Removing VDNType from Callgroup Name
deployed_cgs_relevant[, callgroup := str_remove_all(callgroup, "\\|Skill:.*")] # Filtering SKill Name from Callgroup Name

deployed_cgs_relevant <- distinct(.data = deployed_cgs_relevant, callgroup, .keep_all = TRUE) # Filtering distinct callgroups only after remving VDNType and SKill Name
deployed_cgs_relevant$callgroup <- tolower(deployed_cgs_relevant$callgroup) # Converting Callgroup to Lower Case


######################## Fetching Evallog based on GeneratedOn and Active Models for Repercentiling ########

  ### Eval Log Query to fetch data based on Active Model and GeneratedOn/Sys.Date whichever is greater ###
  evallog <- run_query(paste0("select Callgroup, callguid, calltime, on_off, model, actualvdn
                              from `etl_dev`.`bskybinternal.vevaluatorlog`
                              where calltime >= '", Sys.Date(),"'
                              and Model in ", Model ,"
                              and callgroup not like '%global%' and callgroup not like '%found%';"), "arch_etldev") 
  
  setDT(evallog) # Converting evallog to DT


###################### Filtering Evallog based on Inscope VDNs & Is Current Callgroup ######################
evallog_relevant <- evallog[actualvdn %in% relevantvdns$vdn ]
  
evallog_relevant <- evallog_relevant[, callgroup := str_remove_all(callgroup, "\\|VDNType:.*")] #  Removing VDNType from Callgroup Name
evallog_relevant <- evallog_relevant[, callgroup := str_remove_all(callgroup, "\\|Skill:.*")] # Filtering SKill Name from Callgroup Name
evallog_relevant$callgroup <- tolower(evallog_relevant$callgroup)

evallog_relevant <- evallog_relevant[callgroup %in% deployed_cgs_relevant$callgroup]

########################## Model wise Call Volume For comparing with Threshold defined Above ###############
calls_model <- evallog_relevant[, .(Calls_Model = .N), by = .(Model = model)]
calls_model$Model <- tolower(calls_model$Model)

############################## Production Callgroups From EvalLog #########################################

production_cgs <- evallog_relevant[, .(Calls = .N) , by = .(callgroup) ] # Calculating Callgroup level Call Volume
production_cgs <- production_cgs[, model := str_remove_all(str_extract(callgroup,"_[a-zA-Z]+_"),"_")] # Extracting Model Name from Callgroup Name
production_cgs$model <- gsub( "platform", "platform classic", production_cgs$model) #Subtituing Platform in above Model Column with Platform Classic for consistency
production_cgs$callgroup <- tolower(production_cgs$callgroup) #Converting Callgroups to lower case for consistency


######################################## CG Repercentling Function ################################################
reperc_cg_table <- function(ActiveModels_For_Reperc, Calls_Threshold, production_cgs, calls_model, deployed_cgs_relevant){
  
  ####################### Following Paramters need to be in put to this function ##########################
  
  ###### ActiveModels_For Reperc = All the Active Models on which we need to lapply in order to repercentiled their CGS
  ###### Calls_Threshold = Calls Threshold below which Repercentile wouldnt happen for that Model
  ###### production_cgs = These are production callgroups which are based on evaluator log
  ###### calls_model = These are Call Volume for Active Models based on Evaluator log
  ###### deployed_cgs_relevant = These are deployed callgroups for Active Models which are deployed from Balerion
  
  ########################################################################################################
  
  StartTime <- as.character(Sys.time()) # This to track the Start Time when SP ran for each model 
  tic() # Starting the stop watch to calculate the Execution Time for SP to ran for each model
  
  
  Model_1 <- ActiveModels_For_Reperc
  calls_i <- calls_model[Model %in% Model_1] # Checking Calls for each Model to compare with Threshold
  
  #The following If condition is just an additional check so that if calls_i is empty for any Model its replaced with 0 before it is compared with Calls_Threshold
  if(length(calls_i$Calls_Model)==0){
    calls <- 0 
  }else{
    calls <- calls_i$Calls_Model 
  }
  
  TotalCallsInModel <- calls # This is to track the Total Calls in each model when the SP ran for each model
  
  
  ##########################################################################################################
  
  production_cgs_model <- production_cgs[model %in% Model_1, .(callgroup, Calls, model, Actual_BW = round(100*(Calls/sum(Calls)),1))] # Actual Bandwidth for each callgroup in particular model is calculated from Evallog
  deployed_cgs_model <- deployed_cgs_relevant[model %in% Model_1] # Deployed Callgroup for a particular Model is fetched
  cg_bw_comparison <- production_cgs_model[deployed_cgs_model, on = 'callgroup'] # Deployed CGs and Production CGs are joined with Deployed CGs on Left
  cg_bw_comparison <- cg_bw_comparison[is.na(Actual_BW), Actual_BW := 0] # If any Production Callgroup is missing from Deployed CG we are setting Actual_BW for that callgroup to 0
  
  ########################################### Repercentiling Script ########################################
  
  cg_bw_comparison <- cg_bw_comparison[order(minpercentile), Difference := (expected_bw - Actual_BW)][order(minpercentile), MaxPercentile := cumsum(Calls)/sum(Calls)][order(minpercentile), MinPercentile := lag(MaxPercentile)]
  cg_bw_comparison$MinPercentile <- replace_na(cg_bw_comparison$MinPercentile, 0) # For the first Callgroup we are setting MinPercentle to 0
  
  ########################### Checking If Calls in a Model are Greater Than Threshold Defined ###############
  
  # If calls in a Model are less than Threshold, SP is not ran and deployed cps are returned
  if (calls <= Calls_Threshold){
    
    cg_table_reperc <- deployed_cgs_model[model %in% Model_1] %>% 
      select(callgrouprank, predicaterank, callgroup, predicate, percentile, 
             calls, z, generatedon, iscurrent, vdn, skill, requesttype, minpercentile, 
             maxpercentile, cp_crpercentile, cp_deltapercentile, cp_mixpercentile, 
             cp_cr_actual, cp_delta_actual, percentile_upperlimit, aht, comments, frontncount,
             uniformroutingprob, repercentilegroup, serviceobjective)
    
    #Following 4 Lines are Just for Tracking purpose
    N_CGs_Deployed <- nrow(deployed_cgs_model) # Total Deployed CGs 
    N_CGs_Repercentiled <- 0  # Total Repercentiled CGs
    MinPercentile <- round(min(cg_table_reperc$minpercentile),3) #Min Percentile 
    MaxPercentile <- round(max(cg_table_reperc$maxpercentile),3) #Max Percentile
    Max_BW_Diff <- round(max(cg_bw_comparison$Difference),2) # Max BW Difference
    Mean_BW_Diff <- round(mean(cg_bw_comparison$Difference),2) # Mean BW Difference
    
    
  }else{
    
    
    ########### Final Select Statement to include only those columns require for Production CG table #########
    cg_table_reperc <- cg_bw_comparison[,.(callgrouprank, predicaterank, callgroup, predicate, percentile, 
                                           calls, z, generatedon, iscurrent, vdn, skill, requesttype, minpercentile = MinPercentile, 
                                           maxpercentile = MaxPercentile, cp_crpercentile, cp_deltapercentile, cp_mixpercentile, 
                                           cp_cr_actual, cp_delta_actual, percentile_upperlimit, aht, comments, frontncount,
                                           uniformroutingprob, repercentilegroup, serviceobjective)]
    
    
    N_CGs_Deployed <- nrow(deployed_cgs_model) # Total Deployed CGs 
    N_CGs_Repercentiled <- nrow(cg_table_reperc) # Total Repercentiled CGs
    MinPercentile <- round(min(cg_table_reperc$minpercentile),3) #Min Percentile 
    MaxPercentile <- round(max(cg_table_reperc$maxpercentile),3) #Max Percentile
    Max_BW_Diff <- round(max(cg_bw_comparison$Difference),2) # Max BW Difference
    Mean_BW_Diff <- round(mean(cg_bw_comparison$Difference),2) # Mean BW Difference
    
  }
  
  b <- toc(, quiet = TRUE) # For tracking the elapsed time the SP tooks to ran
  ExecutionTime <- paste0(as.character(round(b$toc - b$tic,3))," sec") # For tracking the Execution Time based on tic() / toc() computed above
  
  # This just contains a data frame where are the tracking related columns are inserted for each model for inserting it later into a MySQL Table
  track_table <- data.frame(Model = Model_1, TotalCallsInModel, Calls_Threshold,  StartTime, ExecutionTime, N_CGs_Deployed, N_CGs_Repercentiled, MinPercentile, MaxPercentile, Max_BW_Diff, Mean_BW_Diff)
  
  return(list(cg_table_reperc,track_table))
  
}


######################## This will call the repercentile function created above###############################
dat <- lapply(ActiveModels_For_Reperc, reperc_cg_table, Calls_Threshold, production_cgs, calls_model, deployed_cgs_relevant)

############### This will create a combined callgroups table for each Model containing all CGs ############### 
dat2 <- lapply(dat, `[[`, 1)
cg_reperc_combined <- rbindlist(dat2)

############ This will create a table with one row for each Model containing all SP tracking Details ######### 
dat2 <- lapply(dat, `[[`, 2)
SP_Track <- rbindlist(dat2)


######## This will Insert the RepercentiledCGS of All Models into a Permanent table for Later Join ##########
run_query("Delete from `aidev`.`bskybinternal_repercentiledcgs.callgroups`", 'arch_aidev')
con<- dbConnect(dbDriver("MySQL"),user="satmap_ai_bskyb", password="$bskyb#123", dbname="aidev", host="10.105.2.57", port = 3307);
dbWriteTable(con, name = "bskybinternal_repercentiledcgs.callgroups", value = cg_reperc_combined , overwrite = F, append = T, row.names = F)


########################### Update Main CG Table with the Repercentled CGs Table ###########################

############## Update Main Callgroups Table in Production with Repercentiled CGs
run_query("UPDATE `aidev`.`bskybinternal_platform_deployedcgs.callgroups` a
          LEFT JOIN  `aidev`.`bskybinternal_repercentiledcgs.callgroups` b
          ON substring_index(a.callgroup,'|VDNType',1) = b.callgroup
          set a.minpercentile = b.minpercentile,
          a.maxpercentile = b.maxpercentile
          where a.iscurrent = 1;", 'arch_aidev')

run_query(" UPDATE `aidev`.`bskybinternal_service_deployedcgs.callgroups` a
            LEFT JOIN  `aidev`.`bskybinternal_repercentiledcgs.callgroups` b
            ON substring_index(substring_index(a.callgroup,'|Skill',1), '|VDNType', 1) = b.callgroup
            set a.minpercentile = b.minpercentile,
            a.maxpercentile = b.maxpercentile
            where a.iscurrent = 1;")


### This will Insert the repercentiling tracker Information in the Table ###################
c <- toc(, quiet = TRUE)
SP_Track$TotalExecutionTime <- paste0(as.character(round(c$toc - c$tic,3))," sec")

dbWriteTable(con, name = "cg_repercentiling_tracker", value = SP_Track , overwrite = F, append = T, row.names = F)

######################### This will close all Open Connections ################################
lapply( dbListConnections( dbDriver( drv = "MySQL")), dbDisconnect)

