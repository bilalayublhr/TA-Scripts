#
library(data.table)
library(easy.db.query)
ap <- "select agentid, percentile, generatedon
from `bskybinternal.agentperformance` 
where generatedon > '2020-06-01'
and split = 'platformclassic'"
ap <- run_query(ap, "prod")
setDT(ap)
ap <- ap[agentid != 'Unknown Agent']
ap <- dcast(ap, agentid ~ generatedon, value.var = "percentile")
cor_matrix <- cor(ap %>% select(-agentid), use = "pairwise.complete.obs", method = "spearman") 
