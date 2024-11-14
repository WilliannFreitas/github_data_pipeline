
CREATE OR REPLACE VIEW silver_vw_repos_por_empresa
as
select 
 ds_company
, sum(vl_public_rep) vl_public_rep
from
	public.bronze_usuarios_github
group by 1
order by 2 desc;

select * from silver_vw_repos_por_empresa



CREATE OR REPLACE VIEW silver_vw_seguidores_por_usuario
as
select 
 ds_login
, sum(vl_followers) vl_followers
, sum(vl_following) vl_following
from
	public.bronze_usuarios_github
where 1 = 1
and dh_update::date >= '2024-11-01'
group by 1
order by 2 desc;



CREATE OR REPLACE VIEW silver_vw_repos_por_localidade
as
select 
 nm_local
, sum(vl_public_rep) vl_public_reps
from
	public.bronze_usuarios_github
where 1 = 1
--and dh_update::date >= '2024-11-01'
group by 1
order by 2 desc;


