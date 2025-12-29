create schema if not exists prd_ahp;

create table if not exists prd_ahp.m_destinos (
  orden integer,
  c_directivo varchar(99),
  u_organica varchar(99),
  c_trabajo varchar(99),
  area varchar(99),
  localidad varchar(99)
);
