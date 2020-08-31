library(easy.db.query)
library(data.table)
library(lubridate)
library(janitor)
library(ggplot2)
library(pheatmap)
library(stringr)
library(dplyr)
library(parallel)
library(plotly)
library(zoo)
library(remotes)
library(tidyr)
library(tidyverse)

original_cgs <- run_query("select substring_index(callgroup,'|VDNType',1) cg, 
                          round((maxpercentile-minpercentile)*100,1) as Expected_BW , minpercentile
                          from `bskybinternal_platform.CallGroups` 
                          where iscurrent=1
                          and callgroup like '%balerion%' group by 1 order by predicaterank", "prod")

eval <- run_query("select CallGroup, cp_actual, evalalgoused,
                        (case when evalalgoused like '%L1%' then 'L1' when evalalgoused like '%L2%' then 'L2' else 'NA' end) as EvalALgo, on_off,
                        substring_index(callgroup,'|VDNType',1) cg, callTime, substr(`VDN`,1,(locate('_PBX',`VDN`) -1)) vq
                        from `engine.evaluatorlog` 
                        where calltime >= curdate()
                        and callgroup not like '%global%' and callgroup not like '%found%';", "arch")

vqarea_mapping <- run_query("select distinct vq_sct
                        from `bskybinternal.vqareamapping` 
                        where area like '%platform%' and  lob like '%platform%';", "arch")

setDT(original_cgs)
setDT(eval)
setDT(vqarea_mapping)

ptf_eval <- eval[vq %in% vqarea_mapping$vq_sct]
ptf_cg_actualbw <- ptf_eval[, .(Calls = .N), by = cg] %>% mutate(Actual_BW = round(100*(Calls/sum(Calls)),1)) %>% setDT()
ptf_cg_bw_comparison <- ptf_cg_actualbw[original_cgs, on="cg"]
ptf_cg_bw_comparison_analysis <- copy(ptf_cg_bw_comparison)
ptf_cg_bw_comparison_analysis <- ptf_cg_bw_comparison_analysis[, cg := str_replace(str_replace(str_replace(str_replace(str_extract(cg, "\\|.*\\|Model"),"\\|",""),"\\|Model", "") , "CG:" , "") , ":.*" ,"")]

## CG BW Difference (Expected - Actual)
ggplot(ptf_cg_bw_comparison, aes(x = reorder(cg, minpercentile), y = (-expected_bw + Actual_BW)))+ geom_bar(stat = 'identity')

## CP_Actual Distribution
ggplot(ptf_eval[evalalgo != "NA"], aes(x = cp_actual))+ geom_density() + facet_wrap(~on_off+evalalgo)


## CP Decile was BW 
CP_df <- ptf_eval[, CP_Bucket := floor(cp_actual*10)+1][!is.na(CP_Bucket), .(Calls = .N, Expected_BW = 10), by = (CP_Bucket)] %>% mutate(Actual_BW = round(100*Calls/sum(Calls),1)) %>% setDT() 
ggplot(CP_df, aes(x = reorder(CP_Bucket, CP_Bucket), y = (-Expected_BW + Actual_BW)))+ geom_bar(stat = 'identity')

#Repercentling Script
setDT(ptf_cg_bw_comparison)
ptf_cg_bw_comparison <- ptf_cg_bw_comparison[order(minpercentile), Difference := (expected_bw - Actual_BW)][order(minpercentile), MaxPercentile := cumsum(Calls)/sum(Calls)][order(minpercentile), MinPercentile := lag(MaxPercentile)]
ptf_cg_bw_comparison$MinPercentile <- replace_na(ptf_cg_bw_comparison$MinPercentile, 0)
ptf_cg_bw_comparison <- ptf_cg_bw_comparison[,!"minpercentile"]

conn <- dbConnect(MySQL(), user = "satmap_ai_bskyb", password = "MM@$346", 
                  dbname = "bskybsatmap", host = "10.105.11.51", 
                  port = 3307)



##### More work needs to be done to send the above table to a temporary table on production server and then create a join with the callgroups table within R and run update statement. 

