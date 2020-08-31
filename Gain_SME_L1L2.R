sme <- run_query("Select week(Calltime) as week, (case when evalalgoused like '%L1%' then 'L1' when evalalgoused like '%L2%' then 'L2' else evalalgoused end) as EvalAlgo
,ifnull((sum(case when on_off = 1 then issavedtv else 0 end)/sum(case when on_off = 1 then 1 else 0 end) - sum(case when on_off = 0 then issavedtv else 0 end)/sum(case when on_off = 0 then 1 else 0 end))*100,0) AbsGain_oncallsave
,ifnull((sum(case when on_off = 1 then issavedtv_trueup else 0 end)/sum(case when on_off = 1 then 1 else 0 end) - sum(case when on_off = 0 then issavedtv_trueup else 0 end)/sum(case when on_off = 0 then 1 else 0 end))*100,0) AbsGain_maturedsave
,ifnull((sum(case when on_off = 1 then revenue else 0 end)/sum(case when on_off = 1 then 1 else 0 end) - sum(case when on_off = 0 then revenue else 0 end)/sum(case when on_off = 0 then 1 else 0 end)),0) AbsGain_oncallrev
,ifnull((sum(case when on_off = 1 then revenue_trueup else 0 end)/sum(case when on_off = 1 then 1 else 0 end) - sum(case when on_off = 0 then revenue_trueup else 0 end)/sum(case when on_off = 0 then 1 else 0 end)),0) AbsGain_maturedrev
,count(*) as calls
from `smexplorerdata`.`bskybinternal.smetable_ptf_new`
where calltime >= '2020-07-01' and calltime <= '2020-08-22'
group by 1,2;", "ai")

sme2 <- melt(data = sme, id.vars = c("week", "evalalgo", "calls"), measure.vars = c("absgain_oncallsave", "absgain_maturedsave", "absgain_oncallrev", "absgain_maturedrev"))

  
ggplot(data = sme2 , aes(x = week , y = value))+
geom_point(aes(size = calls))+ 
geom_line(aes(color = evalalgo))+
  facet_wrap(~variable, scales = "free")
theme(axis.text.x = element_text(angle = 90))+
labs(x = 'Week', y = 'AbsGain', title = 'Gain')


