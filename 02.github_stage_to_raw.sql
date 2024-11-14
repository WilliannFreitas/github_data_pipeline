select
	MD5(coalesce(sug."Login", '0')                  ||
		coalesce(sug."ID", '0')                     ||
		coalesce(sug."Nome", '0')                   ||
		coalesce(sug."Empresa", '0')                ||
		coalesce(sug."Seguidores", '0')             ||
		coalesce(sug."Seguindo", '0')               ||
		coalesce(sug."Repositórios Públicos", '0')  ||
		coalesce(sug."Email", '0') 
	) id_user
	, sug."Login"
	, sug."ID"
	, sug."Nome"
	, sug."Empresa"
	, sug."Localização"
	, sug."Email"
	, sug."Bio"
	, sug."Repositórios Públicos"
	, sug."Seguidores"
	, sug."Seguindo"
	, sug."Criado em"
	, sug."Última atualização"
	, sug."URL Perfil"
	, CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo' as dh_insercao
--into public.raw_usuarios_github 
from public.stage_usuarios_github sug
left join public.raw_usuarios_github rag
on
rag.id_user = 	MD5(coalesce(sug."Login", '0')                  ||
					coalesce(sug."ID", '0')                     ||
					coalesce(sug."Nome", '0')                   ||
					coalesce(sug."Empresa", '0')                ||
					coalesce(sug."Seguidores", '0')             ||
					coalesce(sug."Seguindo", '0')               ||
					coalesce(sug."Repositórios Públicos", '0')  ||
					coalesce(sug."Email", '0') 
					)
where
rag.id_user is null;
