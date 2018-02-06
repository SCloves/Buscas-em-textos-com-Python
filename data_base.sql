create database indice;
create schema buscas_em_textos;


create table buscas_em_textos.urls(
	idurl int not null,
	url varchar(1000) not null,
	constraint pk_urls_idurl primary key (idurl)	
);

create index idx_urls_url on buscas_em_textos.urls(url);

create table buscas_em_textos.palavras(
	idpalavra int not null,
	palavra varchar(200) not null,
	constraint pk_palavras_palavra primary key (idpalavra)
);

create index idx_palavras_palavra on buscas_em_textos.palavras(palavra);

create table buscas_em_textos.palavra_localizacao(
	idpalavra_localizacao int not null,
	idurl int not null,
	idpalavra int not null,
	localizacao int,
	constraint pk_idpalavra_localizacao primary key (idpalavra_localizacao),
	constraint fk_idpalavra_localizacao_idurl 
		foreign key (idurl) references buscas_em_textos.urls(idurl),
	constraint fk_palavra_localizacao_idpalavra
		foreign key (idpalavra) references buscas_em_textos.palavras(idpalavra)
);

create index idx_palavra_localizacao_idpalavra on buscas_em_textos.palavra_localizacao(idpalavra);

alter database indice character set = utf8mb4 collate = utf8mb4_unicode_ci;
alter table palavras convert to character set utf8mb4 collate = utf8mb4_unicode_ci;


