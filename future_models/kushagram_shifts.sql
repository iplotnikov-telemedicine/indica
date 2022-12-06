
delete from looker_tmp.kushagram_shifts where open_at >= DATE_ADD(DATE(NOW()), INTERVAL -7 day);
insert into looker_tmp.kushagram_shifts
select  
    register_log.register_id,
    register.name as register_name,
    register_log.lag_dttm as open_at,
    register_log.cur_dttm as closed_at
    -- TIMESTAMPDIFF(MINUTE, register_log.lag_dttm, register_log.cur_dttm) as dur_minutes
from (
  select
    register_id,
    @reg as lag_reg,
    @reg:=register_id as cur_reg,
    @dttm as lag_dttm,
    @dttm:=created_at as cur_dttm,
    @type as lag_type,
    @type:=type as cur_type
  from c3628_company.register_log
  where type in (1,4)
  	  and created_at >= DATE_ADD(DATE(NOW()), INTERVAL -7 day)
  order by register_id, created_at
) as register_log
inner join c3628_company.register
  on register_log.register_id = register.id
where register_log.cur_type = 4
  and register_log.lag_type = 1
  and register_log.lag_reg = register_log.cur_reg
  and TIMESTAMPDIFF(SECOND, register_log.lag_dttm, register_log.cur_dttm) > 0;