start.date  = '2020-08-17'
end.date    = '2020-08-24' 

library(dplyr)
library(easy.db.query)
library(lubridate)
library(tidyr)

eval <- run_query(paste0("select callguid , substring_index(callgroup, 'VDNType' , 1)  callgroup
                         from `bskybinternal.vevaluatorlog` 
                         where calltime > '", start.date, "' and calltime <'", end.date,"'
                         and area like '%platform%'" ,sep=""), "arch")

vaca <- run_query(paste0("select callguid , callstarttime as calltime , agentid , revenue_trueup  revenue , on_off , EvalAlgoUsed
                         from `vaca_ta` 
                         where callstarttime between '",start.date,"' and '",end.date,"' 
                         and actualvdn in (select vq_sct from `bskybinternal.vqareamapping` where lob like '%platform%') and isrelevant = 1 " , sep=""), "arch") 

merged <- merge(eval , vaca , by= "callguid" )

scores <- merged %>% group_by(dt = date(calltime), agentid, callgroup) %>%
  dplyr::summarise(payoff = sum(revenue)/n() )

library(magrittr)
merged %<>% mutate( dt = date(calltime))


final = merge(merged, scores )
final = final%>% group_by(dt, on_off) %>% dplyr::summarise(score = sum(payoff)/n(), cr = mean(revenue)) 

luck = spread(final%>% select(-cr), on_off, score)
# act = spread(final%>% select(-score), on_off, cr)

luck <- luck %>% mutate( luckadj = (`1` - `0`))
# act <- act %>% mutate( actual = `1` - `0`)

# fin = merge(luck %>% select( luckadj), act %>% select( actual))

fin = luck
fin$luckadj = 1 * fin$luckadj  
# fin$actual = 100 * fin$actual  

fin

