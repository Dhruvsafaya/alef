BEGIN;
create table backups.temp_fact_tutor_suggestions as
    (select a.*
     from (select *, row_number() over (partition by fts_message_id, fts_suggestion_id order by fts_dw_id) as rnk
           from alefdw.fact_tutor_suggestions) a
     where a.rnk > 1);

alter table backups.temp_fact_tutor_suggestions drop column rnk;

delete
from alefdw.fact_tutor_suggestions
where fts_dw_id in
      (select a.fts_dw_id
       from (select *, row_number() over (partition by fts_message_id, fts_suggestion_id order by fts_dw_id) as rnk
             from alefdw.fact_tutor_suggestions) a
       where a.rnk > 1);

insert into alefdw.fact_tutor_suggestions
select *
from backups.temp_fact_tutor_suggestions;

drop table backups.temp_fact_tutor_suggestions;
COMMIT;