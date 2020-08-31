rm(list = setdiff(ls(), c()))
source('~/stan_functions/functions_featureselection.R')

#### 1.1 Loading Libraries ####
library(data.table)
library(ggplot2)
library(DBI)
library(RMySQL)
library(dplyr)
library(gtable)
library(mailR)

#### 1.2 Configuration ####

#Features has all variables that we can use in CG tree
client_name <- "sky"
Features <- c('vdn_bskyb', 'vdngroup_dtv', 
              'modelgroups', 'd_bskyb', 'd4_bskyb', 
              'ri_responsenumber_bskyb', 'ri_processinggroup_bskyb', 'ri_servicecalltype_bskyb',
              'rep_anscomp_bskyb', 'rep_sg3strm_bskyb', 'lbr_orig_bskyb', 'servicesubtype_bskyb', 'screenpop_verified_bskyb', 'screenpop_transfercount_bskyb', 
              'sft_servicecalltype_bskyb', 'priority_bskyb', 'rep_apptag_bskyb', 'ri_loopcounter_bskyb', 'ri_exitcode_bskyb', 'dtv_tenure', 
              'bband_tenure', 'talk_tenure', 'acquisition_channel', 'segment', 'tech_support_count_twelve_months', 'tech_support_count_thirty_days', 
              'dtv_offer_end_dt', 'bband_offer_end_dt', 'talk_offer_end_dt', 'contract_end_dt', 'last_bill_amount', 'payment_status', 'dtv_package', 'q_box',
              'model_id_eval', 'callgroup_eval', 'category', 'ukbtncategory', 'minorcategory', 'queuedskillslist_ecl', 'interaction_type_code', 'ced_apptag_bskyb', 
              'hist_prev_dnis',  
              'hist_prev_area', 'hist_prev_vdngroup', 'hist_first_cli', 'hist_first_dnis', 'hist_first_area', 'hist_first_vdngroup', 
              'hist_prev_ecl_actualvdn', 'hist_prev_ecl_skill', 'hist_prev_ecl_btn', 'hist_prev_d_bskyb', 'hist_prev_ri_responsenumber_bskyb', 'hist_prev_rep_anscomp_bskyb', 
              'hist_prev_screenpop_verified_bskyb', 'hist_prev_screenpop_transfercount_bskyb', 'hist_prev_sft_servicecalltype_bskyb', 'hist_prev_priority_bskyb', 
              'hist_prev_rep_apptag_bskyb', 'hist_first_ecl_actualvdn', 'hist_first_ecl_skill', 'hist_first_ecl_btn', 'hist_first_d_bskyb', 'hist_first_ri_responsenumber_bskyb', 
              'hist_first_rep_anscomp_bskyb', 'hist_first_screenpop_verified_bskyb', 'hist_first_screenpop_transfercount_bskyb', 'hist_first_sft_servicecalltype_bskyb', 
              'hist_first_priority_bskyb', 'hist_first_rep_apptag_bskyb', 'dtv_tenure_original', 'bband_tenure_original', 'talk_tenure_original', 
              'tech_support_count_twelve_months_original', 'tech_support_count_thirty_days_original', 'dtv_offer_end_dt_original', 'bband_offer_end_dt_original', 
              'talk_offer_end_dt_original', 
              'lastcall_area_acth', 'lastcall_issavedtv_acth',  'recent_issale_acth', 'recent_dtv_acth', 'recent_issavedtv_acth', 'recent_bbrecontracts_acth', 'first_calldate_acth', 
              'lastdeptsplit_acth', 'lastcalldate_acth', 'handletime_3_lastcall_acth', 'handletime_4_lastcall_acth', 'handletime_5_lastcall_acth', 
              'days_since_last_call_acth', 'weeks_since_last_call_acth', 'months_since_last_call_acth', 'ncalls_acth', 'ncalls_24hours_acth', 
              'ncalls_48hours_acth', 'ncalls_1week_acth', 'ncalls_1month_acth', 'months_since_last_call_mobile_acth', 'ncalls_mobile_acth', 
              'days_since_last_call_other_acth', 'weeks_since_last_call_other_acth', 'months_since_last_call_other_acth', 'ncalls_other_acth', 
              'months_since_last_call_sales_acth', 'ncalls_sales_acth', 'days_since_last_call_saves_acth', 'months_since_last_call_saves_acth', 'ncalls_saves_acth',
              'days_since_last_call_service_acth', 'weeks_since_last_call_service_acth', 'months_since_last_call_service_acth', 'ncalls_service_acth', 'ncalls_winback_acth', 
              'd6_bskyb', 'd8_bskyb', 'ri4_responsenumber_bskyb', 'ri6_responsenumber_bskyb', 'ri8_responsenumber_bskyb', 'varurl', 'varname', 'varbreadcrumb',
              'varwebapptag', 'previousdepartment', 'hist_prev_ced_apptag_bskyb', 'hist_first_ced_apptag_bskyb',
              'dtv_tenure_orginal', 'bband_tenure_orginal', 'precall_tenure_orginal', 'precall_tenure',
              'last_bill_amount_1', 'last_bill_amount_2', 'last_bill_amount_3', 'last_bill_amount_4', 'tenure_crm', 'rev_value_buckets_crm')

##"var2_tag_adata","var7_tag_adata",

#All Target Metrics e.g. Binomial, Categorical, Revenue etc.
Metrics    <- c( "revenue_trueup",
                 "issavedtv as issave",  
                 "issavedtv_trueup as issavedtv_trueup",
                 "revenue",
                 "issavedtv - issavedtv_trueup as decay,
                 precallrevenue")

# Other columns not to be used as target or in CG tree
Othercols <- c("crm_account_number", "date(calltime) date", "month(calltime)  as month", "calltime", "hour(calltime) hour", "skill",
               "actualagentid", "on_off", 'v_calltime_dt') 

allfeatures <- c(Features,Metrics,Othercols)
allfeatures <- unique(allfeatures)

training.start.date <- '2020-04-01'
training.end.date   <- '2020-06-24'
sme.table.name      <- '`bskybinternal.smetable_ptf_new`'
sme.table.filter    <- "isrelevant = 1 and successful = 1 and (dtv_cancellationdate >= v_calltime_dt or (dtv_cancellationdate) is null)" # Default "1"
#and (t_recoverykey is not null or (t_recoverykey is null and cvip>0.05))
ai.db.username      <- 'asheikh'
ai.db.password      <- '@sheikh#123'
ai.db.name          <- 'smexplorerdata'
ai.db.host          <- '10.107.50.93'
ai.db.port          <- 3307

Target              <- "issavedtv_trueup" 
Targettype          <- "categorical"     #numeric if contineous e.g. revenue, 
#categorical if discrete e.g. issave/issale,
#mixture if mix of binomial and contineous i.e, peak at 0, 1, and other values   e.g. ratio metric


#Emails
# recipients <- c("salmansaeed.khan@afiniti.com","afiniti.ai.virginmedia@afiniti.com") 
# smtp_list  <- list(host.name = "10.92.194.61", port = 25)

######## 2 Data Extract ########
mapping.date   <- training.end.date 
ai_connection  <- dbConnect(MySQL(), user = ai.db.username, password = ai.db.password, dbname = ai.db.name, host = ai.db.host, port = ai.db.port)
query.smetable <- paste0("select ", paste0(c(allfeatures), collapse = ", "), " from ", sme.table.name," 
                         where calltime > '",training.start.date,"' 
                         and calltime < '",training.end.date,"' and ",sme.table.filter,";")
sme_data       <- dbGetQuery(ai_connection, query.smetable)
sme_data[is.na(sme_data)] <- 'NA'
colnames(sme_data) <- tolower(colnames(sme_data))
sme_data           <- as.data.table(sme_data)
sme_data$calltime <- as.POSIXct(sme_data$calltime)
colnames(sme_data)[colnames(sme_data)==Target] <- "metric"


#### 2.1 Data Cleaning ####
# sme_data[, skill_group:=substring(skill,1,3)][, .(vol=.N, metric=mean(metric),
#              xfer_rate= mean(ifelse(next_queue !="NA",1,0)),
#              maxdate=as.Date(max(calltime)), 
#              mindate=as.Date(min(calltime))), by="skill_group"][order(-vol)]
#sme_data <- sme_data[(substring(skill,1,3) %in% c('712','130'))]
# 
# sme_data[, skill_group:=substring(skill,1,3)][, .(vol=.N, metric=mean(metric),
#                                                   xfer_rate= mean(ifelse(next_queue !="NA",1,0)),
#                                                   maxdate=as.Date(max(calltime)), 
#                                                   mindate=as.Date(min(calltime))), by="skill_group"][order(-vol)]
#### 2.2 Data Mapping ####  Merge low volume categories to smalls - All variables should be categorical.
data_4_mapping<- sme_data[calltime<=mapping.date,]
NROW(sme_data)
#### 2.2 Zero Agents ####
# remove_zero_agents = 1
# if(remove_zero_agents==1){
# TotalRecords  <- nrow(sme_data)
# TotalAgents   <- NROW(unique(sme_data[,agentid]))
# 
# DateFilter        <- as.Date(max(sme_data[,calltime])-lubridate::days(60))
# AgentIDs          <- sme_data[, .(vol=.N, metric=mean(metric),
#                                   xfer_rate= mean(ifelse(next_queue !="NA",1,0)),
#                                   maxdate=as.Date(max(calltime)), 
#                                   mindate=as.Date(min(calltime))), by="agentid"][order(-vol)]
# RelevantAgentID   <- AgentIDs[(maxdate>= DateFilter) & vol>25,]
# sdevations        <- 3
# Max               <- mean(RelevantAgentID$metric)+sdevations*sd(RelevantAgentID$metric)
# Min               <- mean(RelevantAgentID$metric)-sdevations*sd(RelevantAgentID$metric)
# ZeroAgents        <- RelevantAgentID[metric>=Max,]
# Verypooragents    <- RelevantAgentID[metric<=Min,]
# RelevantAgentID   <- RelevantAgentID[metric<Max,]
# RelevantAgentID   <- RelevantAgentID[metric>Min,]
# 
# hist(RelevantAgentID$xfer_rate)
# 
# 
# HigerXferRateID <- 0
# #HigerXferRateID <- RelevantAgentID[xfer_rate>=0.2,]
# #RelevantAgentID <- RelevantAgentID[xfer_rate<0.2,]
# 
# sme_data   <- sme_data[agentid %in% RelevantAgentID[,agentid],]
# message("Filter ", DateFilter, ": ", 100-100*round(nrow(sme_data)/TotalRecords,2), 
#         "% Calls Removed. ", round(100-100*NROW(unique(sme_data[,agentid]))/TotalAgents,2), "% Agents Removed.",
#         " Total ZeroAgents = ", NROW(ZeroAgents),
#         " Total PoorAgents = ", NROW(Verypooragents),
#         " High Xferagents = ",  NROW(HigerXferRateID))
# } 
# 
# nrow(sme_data)
#### 2.3 Mapping Table for smalls ####
merge_small_categories = 1
if(merge_small_categories ==1){
  callsthreshold = floor(2*NROW(data_4_mapping)/100)
  dictionary     = as.data.frame(Features)
  colnames(dictionary)      <- "column_name"
  dictionary$is_call        <-  T 
  dictionary$is_modelling   <-  T
  dictionary                <- as.data.table(dictionary)
  Processdata               <- DataProcess(preprocessdata=1, 
                                           data_for_mapping=data_4_mapping, 
                                           data_to_process=copy(sme_data),
                                           dictionary=dictionary, 
                                           callsthreshold)
  sme_data      <- Processdata[[1]]
  smallsmapping <- Processdata[[2]]
}

#### 3.1 Balance Data ####
BalanceData=1
Target <- "metric"  
Levels <- DataFrameSumary(as.data.frame(sme_data))
callgroup.data.variables       <- c(Features,c(Target))
callgroup.training.data        <- as.data.frame(sme_data[, ..callgroup.data.variables])
callgroup.training.data        <- callgroup.training.data%>%dplyr::select(colnames(callgroup.training.data)[colnames(callgroup.training.data) %in% Levels$Cols[Levels$n>1]])
callgroup.training.data[,!(colnames(callgroup.training.data)==Target)]  <- data.frame(apply(callgroup.training.data[,!(colnames(callgroup.training.data)==Target)],2,as.factor))

if(BalanceData==1){
  Boosting  <- 1
  TotalData <- 1.0*NROW(sme_data)
  if(Targettype=="numeric"){
    print("numeric")
    callgroup.training.data$metric <- as.numeric(callgroup.training.data$metric)
    All_Cat0 <- callgroup.training.data 
    Cat0     <- dplyr::sample_n(All_Cat0,Boosting*TotalData-NROW(All_Cat0), replace=TRUE)
    callgroup.training.data.balanced <- rbind(All_Cat0,Cat0)
  } else if (Targettype=="categorical") {
    print("categorical") 
    callgroup.training.data$metric <- as.factor(callgroup.training.data$metric)
    callgroup.training.data.balanced <-  data.frame()
    for(ii in unique(callgroup.training.data$metric)){
      message(ii)
      All_Cat0 <- callgroup.training.data%>%dplyr::filter(metric==ii)
      Cat0     <- dplyr::sample_n(All_Cat0,Boosting*TotalData-NROW(All_Cat0), replace=TRUE)
      callgroup.training.data.balanced <- rbind(All_Cat0,Cat0,callgroup.training.data.balanced)
    }
  } else if (Targettype=="mixture") {
    print("mixture") 
    callgroup.training.data$metric <- as.numeric(callgroup.training.data$metric)
    All_Cat0 <- callgroup.training.data%>%dplyr::filter(metric==0)
    All_Cat1 <- callgroup.training.data%>%dplyr::filter(metric==1)
    All_Cat2 <- callgroup.training.data%>%dplyr::filter(metric!=0 & metric !=1)
    
    Cat0     <- dplyr::sample_n(All_Cat0,Boosting*TotalData-NROW(All_Cat0), replace=TRUE)
    Cat1     <- dplyr::sample_n(All_Cat1,Boosting*TotalData-NROW(All_Cat1), replace=TRUE)
    Cat2     <- dplyr::sample_n(All_Cat2,Boosting*TotalData-NROW(All_Cat2), replace=TRUE)
    callgroup.training.data.balanced <- rbind(All_Cat0,All_Cat1,All_Cat2,Cat0,Cat1,Cat2)
  }
  
}
NROW(callgroup.training.data.balanced)/NROW(callgroup.training.data)
callgroup.training.data.actual <- callgroup.training.data
callgroup.training.data        <- callgroup.training.data.balanced

#### 3.2 Dummify Data ####
DummifyData = 0 #Keep this 0
if(DummifyData==1){
  #Levels <- DataFrameSumary(as.data.frame(callgroup.training.data))
  df      <- callgroup.training.data 
  dummy   <- caret::dummyVars(~.,df, fullRank=FALSE,sep="__")
  dummydf <- predict(dummy,df)
  callgroup.training.data <-as.data.frame(dummydf)
  #Levels0 <- DataFrameSumary(as.data.frame(dummydf))
  #summary(dummydf)
  names(callgroup.training.data) <- gsub(" ","_1_",names(callgroup.training.data))
  names(callgroup.training.data) <- gsub(",","_2_",names(callgroup.training.data))
  names(callgroup.training.data) <- gsub(":","_3_",names(callgroup.training.data))
  names(callgroup.training.data) <- gsub("\\|","_4_",names(callgroup.training.data))
  names(callgroup.training.data) <- gsub("\\+","_5_",names(callgroup.training.data))
  names(callgroup.training.data) <- gsub("\\/","_6_",names(callgroup.training.data))
  names(callgroup.training.data) <- gsub("<","_7_",names(callgroup.training.data))
  names(callgroup.training.data) <- gsub("-","_8_",names(callgroup.training.data))
}


#### 3.3 Ranger Random Forrest ####
## First Iteration RF:
num.trees   <- 150     #No of Trees
mtry        <- 0.5      #Fraction of variables to possibly split at each node

ijk=40
for(ijk in seq(140,50,by=-15)){
  print(paste0("Start = ", ijk))
  variableImp <- RFfunction(callgroup.training.data,
                            num.trees=num.trees,
                            No_of_columns_Split=round(mtry*(NCOL(callgroup.training.data) -1)),
                            TotalBalancedData=TotalData*NROW(callgroup.training.data)/NROW(callgroup.training.data.actual))
  SelectedFeatures         <- variableImp%>%dplyr::top_n(ijk,importance)
  callgroup.training.data  <- callgroup.training.data%>%dplyr::select(c(as.character(SelectedFeatures$variable),"metric")) 
  
  print(paste0("End = ", ijk))
}

colnames(callgroup.training.data)[!(colnames(callgroup.training.data) %in% as.character(SelectedFeatures$variable))]
as.character(SelectedFeatures$variable)[!(as.character(SelectedFeatures$variable) %in% colnames(callgroup.training.data))]

variableImp <-  RFfunction(callgroup.training.data,
                           num.trees=num.trees,
                           No_of_columns_Split=round(mtry*(NCOL(callgroup.training.data) -1)),
                           TotalBalancedData=TotalData*NROW(callgroup.training.data)/NROW(callgroup.training.data.actual))

FirstIterationVariableImp <- variableImp
FirstIterationResults     <- SelectedFeatures

## Second Iteration RF
#Remove High Correlation Variables
Remove <-1
while(NROW(Remove)>=1){
  Threshold                <- 0.65   # Discard one of the feature where correlation >Threshold
  Highcorrelationfeatures  <- CorrelationFunction(callgroup.training.data, Threshold,variableImp)
  Colss                    <- colnames(callgroup.training.data)
  Remove                   <- Highcorrelationfeatures[Highcorrelationfeatures %in% Colss]
  print(Remove)
  if(NROW(Remove)>=1){
    callgroup.training.data  <- callgroup.training.data%>%dplyr::select(-Remove)
  }}


## Final Iterations
for(ijk in seq(40,20,by=-5)){
  print(paste0("Start = ", ijk))
  variableImp <- RFfunction(callgroup.training.data,
                            num.trees=num.trees,
                            No_of_columns_Split=round(mtry*(NCOL(callgroup.training.data) -1)),
                            TotalBalancedData=TotalData*NROW(callgroup.training.data)/NROW(callgroup.training.data.actual))
  SelectedFeatures         <- variableImp%>%dplyr::arrange(desc(importance))%>%dplyr::top_n(ijk,importance)
  callgroup.training.data  <- callgroup.training.data%>%dplyr::select(c(as.character(SelectedFeatures$variable),"metric")) 
  
  print(paste0("End = ", ijk))
}



colnames(callgroup.training.data)[!(colnames(callgroup.training.data) %in% as.character(SelectedFeatures$variable))]
as.character(SelectedFeatures$variable)[!(as.character(SelectedFeatures$variable) %in% colnames(callgroup.training.data))]

#Define set of callgroup variables for which you want to run the analysis
other.cgvariables <- Variable_Profiling(sme_data,as.character(SelectedFeatures$variable))
setwd("~/")
file_name= paste0("FeatureSelection_", Target, ".pdf")
pdf( file_name, height = 9, width = 20) 

g <- myTableGrob(SelectedFeatures, "Shortlisted Features")
grid::grid.newpage()
grid::grid.draw(g)

print(ggplot(variableImp%>%dplyr::top_n(20,importance)%>%dplyr::arrange(desc(importance)), aes(x=reorder(variable,importance), y=importance,fill=importance))+ 
        geom_bar(stat="identity", position="dodge")+ coord_flip()+
        ylab("Variable Importance")+xlab("")+  ggtitle("Information Value Summary")+
        guides(fill=F)+
        scale_fill_gradient(low="red", high="blue"))

Correlation_Matrix <- GoodmanKruskal::GKtauDataframe(callgroup.training.data, dgts = 5)
print(plot(Correlation_Matrix, diagSize = 0.8))

for( i in SelectedFeatures$variable){
  
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



# sender <- "AIJOBS@afiniti.com" # Replace with a valid address
# email <- send.mail(from   = sender,
#                    to     = recipients,
#                    subject=paste0(client_name, " | Feature Selection - ",
#                                   sme.table.name, " from ",
#                                   training.start.date, " to ",
#                                   training.end.date),
#                    body   = paste("<html><head><title> Runtime checks</title> </head><body></html>"),
#                    smtp   = smtp_list,
#                    html   = TRUE,
#                    authenticate = FALSE,
#                    send = FALSE,
#                    attach.files = c(file_name),
#                    file.names   = c(file_name)
# )
# email$send()

message("Top N Features in Order of Importance")
dput(as.character(SelectedFeatures$variable))

# VMCare
# #Issave
# c("cp_model_score_int_10", "disc_day_bucket", "digitsdialed", 
#   "cp_products_telco_tier", "tenure_month_bin", "user_gw_cust_active_adata", 
#   "user_dec_1_adata", "user_gw_cust_work_adata", "user_collections_segment_adata", 
#   "end_bucket", "revenue_bucket", "var2_tag_adata", "user_gw_postcode_adata", 
#   "cp_stb_count", "lastleg_vdn", "cp_tenure_bb_tv", "cp_customer_category", 
#   "cp_revenue_bucket", "cp_products_tier", "months_since_last_call_cable_cr_btnh"
# )
# 
# #CVIP
# c("cp_model_score_int_10", "disc_day_bucket", "cp_products_telco_tier", 
#   "end_bucket", "user_gw_cust_active_adata", "disc_perc_bucket", 
#   "user_gw_cust_work_adata", "closestdiscbucket", "cp_dim_tv_tenure_band_key", 
#   "user_gw_cust_tenure_adata", "user_collections_segment_adata", 
#   "cp_dim_telco_tenure_band_key", "user_dec_1_adata", "amountexp60_bucket", 
#   "digitsdialed", "user_gw_postcode_adata", "cp_bb_tv_tier", "var2_tag_adata", 
#   "cp_primary_collection_desc", "months_since_last_call_cable_cr_btnh"
# )