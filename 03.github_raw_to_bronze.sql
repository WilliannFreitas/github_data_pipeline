truncate table public.bronze_usuarios_github;

insert into public.bronze_usuarios_github
select
	id_user,
	"Login" ds_login,
	"ID" id_user_git,
	"Nome" nm_user,
	"Empresa" ds_company,
	"Localização" nm_local,
	"Email" ds_email,
	"Bio" ds_bio,
	"Repositórios Públicos" vl_public_rep,
	"Seguidores" vl_followers,
	"Seguindo" vl_following,
	"Criado em"::timestamp dh_created,
	"Última atualização"::timestamp dh_update,
	"URL Perfil" ds_url_perfil,
	CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo' as dh_insercao
--into public.bronze_usuarios_github
from
	public.raw_usuarios_github;