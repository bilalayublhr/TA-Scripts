library(easy.db.query)
library(dplyr)
library(data.table)
library(ggplot2)
library(pheatmap)
library(stringr)
source("~/balerion/src/main/R/process_model.R")


client.name <- "sky"
queue.name <- "retention" 
model.name <- "platform"
model.number <- "13_C_O"   
opt_metric <- "issavedtv"


cg_vars = c("rev_value_buckets_crm", "case when (hist_prev_ecl_skill in ('','NA') or substring_index(hist_prev_ecl_Skill,'_',3) like '%@%') then hist_prev_ecl_skill else concat(substring_index(hist_prev_ecl_Skill,'_',3), '_*') end as modelgroups2", "case when rep_apptag_bskyb in('','NA') then hist_first_rep_apptag_bskyb else rep_apptag_bskyb end as rep_apptag_bskyb1", "last_bill_amount", "dtv_tenure","hist_first_ecl_actualvdn", "hist_first_rep_apptag_bskyb" , "hist_prev_ecl_Skill", "rep_apptag_bskyb", "vdn_bskyb" , "q_box", "dtv_package", "bband_tenure", "varbreadcrumb", "ced_apptag_bskyb", "previousdepartment", "segment")

cg.select <- paste(cg_vars, collapse = ", ")

sme.data.query <- sprintf("select %s, calltime,  callguid, issavedtv as  opt_metric, on_off, salesmatchedfilter
                          from `bskybinternal.smetable_ptf_new` 
                          where calltime > '2020-07-01' and calltime < '2020-08-21' 
                          and isrelevant = 1 ;", cg.select)
sme.data <- run_query(sme.data.query, "ai")

setDT(sme.data)


time_grouping <- "day"

sme.data[, date := lubridate::date(calltime)]

if(tolower(time_grouping) == 'month'){
  sme.data[, time_grouped := format(date, "%Y-%m")]
} else if(tolower(time_grouping) == 'year'){
  sme.data[, time_grouped := format(date, "%Y")]
} else if(tolower(time_grouping) == 'week'){
  sme.data[, time_grouped := format(date, format = "%Y-%V")]
} else{
  sme.data[, time_grouped := format(date, "%Y-%m-%d")]
}


previous_model_cgs <- readRDS(file =  , paste0("~/balerion_models/", queue.name, "/",model.name , "/models/", model.number,"/callgroup_tree_bin.RDS") )

cg_variables <- attr(previous_model_cgs$terms , "term.labels")

mapping_table <- #readRDS(file = , paste0("training_mapping.RDS"))  
  readRDS(file =  , paste0("~/balerion_models/", queue.name, "/",model.name , "/models/", model.number,"/data/training_mapping.RDS") )

### Converting all callgroup variables in sme.data to character 

cg_variables_sme <- unlist(strsplit(cg_variables, '_ix'))
sme.data <- as.data.frame(sme.data)
sme.data[cg_variables_sme] <- sapply(sme.data[cg_variables_sme], as.character)
setDT(sme.data)

#########

for (col in cg_variables) {
  col <- strsplit(col, '_ix')[[1]]
  sme.data <- merge(x = sme.data , y = mapping_table[[col]] , by = col , all.x = TRUE)
  
}

sme.data <- AttachCallGroup(sme.data, previous_model_cgs )
sme.data <- sme.data[callgroup %in% cg_relevant]


cg.data <- sme.data %>% filter(!is.na(callgroup)) %>% group_by(time_grouped , var = callgroup) %>%
  dplyr::summarise(  calls = n() , cr = mean(opt_metric)) 


if("plyr" %in% (.packages())){
  detach("package:plyr", unload=TRUE) 
}  


cg.data <- cg.data %>% group_by(time_grouped )  %>% mutate(rank = order(order(cr)))

# RPC Correlation
cg_cr_corr.data <- (dcast(cg.data ,  var ~time_grouped  , value.var = "cr" ))
cg_cr_corr.data <- cg_cr_corr.data %>% select(-var) 
cr_correlation_matrix <- cor(cg_cr_corr.data , method = "spearman" , use = "pairwise.complete.obs")

# Rank Correlation
cg_rank_corr.data <- (dcast(cg.data ,  var ~ time_grouped  , value.var = "rank" ))
cg_rank_corr.data <- cg_rank_corr.data %>% select(-var) 
rank_correlation_matrix <- cor(cg_rank_corr.data , method = "spearman" , use = "pairwise.complete.obs")


#BW Correlation
cg_bw_corr.data <- (dcast(cg.data ,  var ~time_grouped  , value.var = "calls" ))
cg_bw_corr.data <- cg_bw_corr.data %>% select(-var) 
bw_correlation_matrix <- cor(cg_bw_corr.data , method = "spearman" , use = "pairwise.complete.obs")

#setwd("~/")
#pdf(paste("New_CG_Var_Trends.pdf")   , height = 9, width = 20)

# a <- ggplot(cg.data , aes(x = (time_grouped ) , y = calls , fill = as.factor(var) )) + geom_bar(stat = "identity" , position = "fill" , color = "black")+
#   ggtitle( " Callgroup Week Wise Call Volume") #+ theme(axis.text.x = element_text(angle = 90)) 
# 
# a
# 
# b <-  ggplot(cg.data , aes(x = time_grouped  , y = cr)) +
#   geom_line(aes(group = as.factor(var) ,  color = as.factor(var)))+ geom_point()+
#   ggtitle("Callgroup Week Wise CR") #+ theme(axis.text.x = element_text(angle = 90)) 
# 
# b
# 
# c <-  ggplot(cg.data  , aes(x = time_grouped  , y = rank)) +
#   geom_line(aes(group = as.factor(var) ,  color = as.factor(var)))+ geom_point()+
#   ggtitle("Callgroup Week Wise Rank") #+ theme(axis.text.x = element_text(angle = 90)) 
model.number <- "3_C_I"

# RPC Correlation Heat Map
d <- pheatmap(cr_correlation_matrix[] ,  cluster_rows = FALSE, cluster_cols = FALSE , display_numbers = TRUE , color = colorRampPalette(c("red", "yellow", "green"))(n = 29) 
         , fontsize_number = 11
         , border_color = "black"  , angle_col = 90 , main = paste0("Model ", model.number ," Callgroup RPC Correlation " , time_grouping, " Wise"))


# Rank Correlation Heat Map
e <- pheatmap(rank_correlation_matrix ,  cluster_rows = FALSE, cluster_cols = FALSE , display_numbers = TRUE , color = colorRampPalette(c("red", "yellow", "green"))(n = 29) 
         , fontsize_number = 11
         , border_color = "black"  , angle_col = 90 , main = paste0("Model ", model.number ," Callgroup Rank Correlation " , time_grouping, " Wise"))

f <- pheatmap(bw_correlation_matrix ,  cluster_rows = FALSE, cluster_cols = FALSE , display_numbers = TRUE , color = colorRampPalette(c("red", "yellow", "green"))(n = 29) 
              , fontsize_number = 11
              , border_color = "black"  , angle_col = 90 , main = paste0("Model ", model.number ," Callgroup BW Correlation " , time_grouping, " Wise"))


 View(bw_correlation_matrix)
 # alpha <- list()
 # alpha <- c(a,b,c,d,e)  
 #  
 #  gridExtra::grid.arrange( grobs = list(a,b,c,d,e),nrow = 3, top="Main Title")
 #  
 #  dev.off()
 #  
