import psycopg2
import pandas as pd
import os
from datetime import date, datetime
import difflib
import numpy as np


def conecta_esus():
  con = psycopg2.connect(host='172.17.135.142', 
                         database='esus',
                         port='5433',
                         user='postgres', 
                         password='esus')
  return con

def conecta_sinan():
  con = psycopg2.connect(host='10.10.5.106', 
                         database='sinanpop92',
                         port='5445',
                         user='postgres')
  return con

#itb producao-------------------------------------------------
def conecta_itb():
  con = psycopg2.connect(host='172.17.135.151', 
                         database='serverdados',
                         port='5433',
                         user='postgres',
                         password='esus')
  return con

#itb desenv--------------------------------------------------
def conecta_itb_d():
  con = psycopg2.connect(host='10.10.5.100', 
                         database='serverdados',
                         port='5433',
                         user='postgres',
                         password='esus')
  return con


#consulta esus------------------------------------------------
def consultar_db(sql):
  con = conecta_esus()
  cur = con.cursor()
  cur.execute(sql)
  recset = cur.fetchall()
  registros = []
  for rec in recset:
    registros.append(rec)
    cur.close()
  con.close()
  return registros

#operacoes itb------------------------------------------------
def executar_db(sql):
  con = conecta_itb()
  cur = con.cursor()
  cur.execute(sql)
  con.commit()
  con.close()

def consultar_itb(sql):
  con = conecta_itb()
  cur = con.cursor()
  cur.execute(sql)
  recset = cur.fetchall()
  registros = []
  for rec in recset:
    registros.append(rec)
    cur.close()
  con.close()
  return registros


def inserir_db(sql):
    con = conecta_itb()
    cur = con.cursor()
    try:
        cur.execute(sql)
        con.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        con.rollback()
        cur.close()
        return 1
    cur.close()

#consulta sinan--------------------------------------------------------
def consultar_dbsn(sql):
  con = conecta_sinan()
  cur = con.cursor()
  cur.execute(sql)
  recset = cur.fetchall()
  registros = []
  for rec in recset:
    registros.append(rec)
    cur.close()
  con.close()
  return registros

#data e hora para print
dth_now = datetime.now()
dth = dth_now.strftime('%d/%m/%Y %H:%M')

# ******************** CRIAÇÃO DO DIRETÓRIO PARA OS ARQUIVOS CSV  ***********************   

#dataatu = date.today()
#datadir = dataatu.strftime('%d%m%Y')

#caminho = 'C:\\tuberc_csv\\'
#diretorio = datadir
#dir = caminho + diretorio + '\\'

#if os.path.exists(dir) == False:
#    os.makedirs(dir)


#dsinan = dir+"\sinan_pac_tb.csv"
#df_bd_sinan.to_csv(dsinan, sep =';',index=False)


#sql = "--pacientes_esus"-------------------------------------------------------
sql = '''select distinct cp.co_seq_fat_cidadao_pec,cp.no_cidadao,upper(c.no_mae) as nome_mae,date(dtn.dt_registro) as dt_nasc, '''
sql += '''cp.nu_cns,cp.nu_cpf_cidadao,concat (upper(tl.no_tipo_logradouro),' ',upper(c.ds_logradouro)) as no_end,'''
sql += '''c.nu_numero, upper(c.no_bairro) as bairro_desc,c.ds_cep, c.nu_telefone_celular, c.nu_telefone_residencial, '''
sql += '''c.nu_telefone_contato, ce.nu_cnes, tu.no_unidade_saude, ce.nu_ine, te.no_equipe from tb_fat_cidadao_pec cp '''
sql += '''join tb_dim_tempo dtn on dtn.co_seq_dim_tempo = cp.co_dim_tempo_nascimento '''
sql += '''join ( select distinct fai.co_fat_cidadao_pec from tb_fat_atendimento_individual fai '''
sql += '''where (fai.ds_filtro_cids LIKE ANY (array['%A150%','%A151%','%A152%','%A153%','%A155%','%A157%','%A158%','%A159%']) or '''
sql += '''(fai.ds_filtro_ciaps like '%A70%' or fai.ds_filtro_ciaps like '%ABP017%')) and fai.co_dim_tempo > 20200000 )  '''
sql += '''tub on tub.co_fat_cidadao_pec = cp.co_seq_fat_cidadao_pec '''
sql += '''join tb_cidadao c on c.co_seq_cidadao = cp.co_cidadao '''
sql += '''join tb_tipo_logradouro tl on tl.co_tipo_logradouro = c.tp_logradouro '''
sql += '''left join tb_cidadao_vinculacao_equipe ce on ce.co_cidadao = c.co_seq_cidadao '''
sql += '''left join tb_equipe te on te.nu_ine =ce.nu_ine '''
sql += '''left join tb_unidade_saude tu on tu.nu_cnes = ce.nu_cnes '''
sql += '''where (te.st_ativo = 1 or te.st_ativo is null); '''

reg = consultar_db(sql)

df_bd_esus = pd.DataFrame(reg, columns=['co_seq_fat_cidadao_pec','no_cidadao','nome_mae','dt_nasc',
'nu_cns','nu_cpf_cidadao','no_end','nu_numero','bairro_desc','ds_cep','nu_telefone_celular','nu_telefone_residencial',
'nu_telefone_contato','nu_cnes','no_unidade_saude','nu_ine','no_equipe'])

sql = '''delete from tb_tuber_esus_old'''
executar_db(sql)

dth_now = datetime.now()
dth = dth_now.strftime('%d/%m/%Y %H:%M')
print(dth + ' ********** REGISTROS TUBER_ESUS ANTIGOS DELETADOS ************')

coni = conecta_itb()
curi = coni.cursor()

for i in df_bd_esus.index:
    sql = """
    INSERT into tb_tuber_esus_old (co_seq_fat_cidadao_pec,no_cidadao,nome_mae,dt_nasc,nu_cns,nu_cpf_cidadao,no_end,nu_numero,bairro_desc,ds_cep,nu_telefone_celular,nu_telefone_residencial,
    nu_telefone_contato,nu_cnes,no_unidade_saude,nu_ine,no_equipe) values('%s',$$%s$$,$$%s$$,'%s','%s','%s',$$%s$$,'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');
    """ % (df_bd_esus['co_seq_fat_cidadao_pec'][i], df_bd_esus['no_cidadao'][i], df_bd_esus['nome_mae'][i], df_bd_esus['dt_nasc'][i], df_bd_esus['nu_cns'][i], df_bd_esus['nu_cpf_cidadao'][i], 
           df_bd_esus['no_end'][i], df_bd_esus['nu_numero'][i], df_bd_esus['bairro_desc'][i], df_bd_esus['ds_cep'][i], df_bd_esus['nu_telefone_celular'][i], 
           df_bd_esus['nu_telefone_residencial'][i], df_bd_esus['nu_telefone_contato'][i], df_bd_esus['nu_cnes'][i], df_bd_esus['no_unidade_saude'][i], df_bd_esus['nu_ine'][i], 
           df_bd_esus['no_equipe'][i])
    try:
        curi.execute(sql)
        coni.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        coni.rollback()
        curi.close()
        break
curi.close()

dth_now = datetime.now()
dth = dth_now.strftime('%d/%m/%Y %H:%M')
print(dth + ' ********** REGISTROS TUBER_ESUS ATUALIZADOS ************')



#sql = "--pacientes_sinan"----------------------------------------------------
sql = """select tn.nu_notificacao,tn.dt_notificacao,tn.co_municipio_notificacao,un.co_cnes,una.co_cnes as cnes_atual,un.ds_estabelecimento, 
una.ds_estabelecimento as ds_estab_atual,tn.tp_notificacao,tn.dt_diagnostico_sintoma,tb.co_cid,tb.dt_notificacao_atual, 
dbsinan.decriptografanova(no_nome_paciente) as nome_pac,dbsinan.decriptografanova(no_nome_mae) as nome_mae,tn.dt_nascimento, 
tn.tp_sexo,tn.nu_cartao_sus,tn.ds_chave_fonetica,tn.co_municipio_residencia,tn.no_logradouro_residencia, 
case when tn.nu_residencia like '%$%' then translate(tn.nu_residencia,'$', '') else tn.nu_residencia end as nu_residencia,
tn.no_bairro_residencia,tn.co_bairro_residencia,tn.nu_cep_residencia,dbsinan.decriptografanova(tn.nu_ddd_residencia) as nu_ddd, 
dbsinan.decriptografanova(tn.nu_telefone_residencia) as nu_tel,tb.tp_entrada,tb.dt_inicio_tratamento,tb.nu_contato, 
tb.nu_contato_examinado,tb.tp_hiv,tb.tp_situacao_encerramento,tb.dt_encerramento,tb.tp_transf,tb.tp_forma, 
tb.tp_extrapulmonar_1,tb.tp_extrapulmonar_2,tb.st_baciloscopia_escarro,tb.st_baciloscopia_escarro2,tb.tp_cultura_escarro, 
tb.tp_cultura_outro,tb.tp_histopatologia,tb.tp_raio_x,tb.tp_molecular,tb.tp_tratamento,tb.st_baciloscopia_1_mes, 
tb.st_baciloscopia_2_mes,tb.st_baciloscopia_3_mes,tb.st_baciloscopia_4_mes,tb.st_baciloscopia_5_mes,tb.st_baciloscopia_6_mes, 
tb.st_bacil_apos_6_mes,tb.tp_pop_imigrante,tb.tp_pop_liberdade,tb.tp_pop_rua,tb.tp_pop_saude,tb.st_agravo_aids,tb.st_agravo_alcolismo, 
tb.st_agravo_diabete,tb.st_agravo_drogas,tb.st_agravo_mental,tb.st_agravo_outro,tb.st_agravo_tabaco,tb.tp_antirretroviral_trat, 
tb.tp_sensibilidade,tb.tp_tratamento_acompanhamento from dbsinan.tb_notificacao tn  
join dbsinan.tb_investiga_tuberculose tb on tb.nu_notificacao = tn.nu_notificacao and tb.dt_notificacao = tn.dt_notificacao  
join dblocalidade.tb_estabelecimento_saude un on un.co_estabelecimento = tn.co_unidade_notificacao  
join dblocalidade.tb_estabelecimento_saude una on una.co_estabelecimento = tb.co_unidade_saude_atual 
join 
(	--ultima notif
	select
	dbsinan.decriptografanova(no_nome_paciente) as nome_p,
	nt.dt_nascimento,
	max(nt.nu_notificacao::text||nt.dt_notificacao::text) as idnoti
	from dbsinan.tb_notificacao nt 
	join dbsinan.tb_investiga_tuberculose tu on tu.nu_notificacao = nt.nu_notificacao and tu.dt_notificacao = nt.dt_notificacao 
	where tu.co_cid = 'A16.9'
	group by 1,2
) ultn on ultn.idnoti = (tn.nu_notificacao::text||tn.dt_notificacao::text)
where tb.co_cid = 'A16.9';"""

reg = consultar_dbsn(sql)

df_bd_sinan = pd.DataFrame(reg, columns=['nu_notificacao','dt_notificacao','co_municipio_notificacao','co_cnes','cnes_atual','ds_estabelecimento',
'ds_estab_atual','tp_notificacao','dt_diagnostico_sintoma','co_cid','dt_notificacao_atual',
'nome_pac','nome_mae','dt_nascimento','tp_sexo','nu_cartao_sus','ds_chave_fonetica','co_municipio_residencia','no_logradouro_residencia','nu_residencia',
'no_bairro_residencia','co_bairro_residencia','nu_cep_residencia','nu_ddd','nu_tel','tp_entrada','dt_inicio_tratamento','nu_contato',
'nu_contato_examinado','tp_hiv','tp_situacao_encerramento','dt_encerramento','tp_transf','tp_forma',
'tp_extrapulmonar_1','tp_extrapulmonar_2','st_baciloscopia_escarro','st_baciloscopia_escarro2','tp_cultura_escarro',
'tp_cultura_outro','tp_histopatologia','tp_raio_x','tp_molecular','tp_tratamento','st_baciloscopia_1_mes',
'st_baciloscopia_2_mes','st_baciloscopia_3_mes','st_baciloscopia_4_mes','st_baciloscopia_5_mes','st_baciloscopia_6_mes',
'st_bacil_apos_6_mes','tp_pop_imigrante','tp_pop_liberdade','tp_pop_rua','tp_pop_saude','st_agravo_aids','st_agravo_alcolismo',
'st_agravo_diabete','st_agravo_drogas','st_agravo_mental','st_agravo_outro','st_agravo_tabaco','tp_antirretroviral_trat',
'tp_sensibilidade','tp_tratamento_acompanhamento'])

df_bd_sinan = df_bd_sinan.replace([None],'')

sql = '''delete from tb_tuber_sinan_old'''
executar_db(sql)

dth_now = datetime.now()
dth = dth_now.strftime('%d/%m/%Y %H:%M')
print(dth + ' ********** REGISTROS TUBER_SINAN ANTIGOS DELETADOS ************')

coni = conecta_itb()
curi = coni.cursor()


for i in df_bd_sinan.index:
    sql = """
    INSERT into tb_tuber_sinan_old (nu_notificacao,dt_notificacao,co_municipio_notificacao,co_cnes,cnes_atual,ds_estabelecimento,ds_estab_atual,tp_notificacao,dt_diagnostico_sintoma,
    co_cid,dt_notificacao_atual,nome_pac,nome_mae,dt_nascimento,tp_sexo,nu_cartao_sus,ds_chave_fonetica,co_municipio_residencia,no_logradouro_residencia,
    nu_residencia,no_bairro_residencia,co_bairro_residencia,nu_cep_residencia,nu_ddd,nu_tel,tp_entrada,dt_inicio_tratamento,nu_contato,nu_contato_examinado,tp_hiv,tp_situacao_encerramento,
    dt_encerramento,tp_transf,tp_forma,tp_extrapulmonar_1,tp_extrapulmonar_2,st_baciloscopia_escarro,st_baciloscopia_escarro2,tp_cultura_escarro,tp_cultura_outro,
    tp_histopatologia,tp_raio_x,tp_molecular,tp_tratamento,st_baciloscopia_1_mes,st_baciloscopia_2_mes,st_baciloscopia_3_mes,st_baciloscopia_4_mes,st_baciloscopia_5_mes,
    st_baciloscopia_6_mes,st_bacil_apos_6_mes,tp_pop_imigrante,tp_pop_liberdade,tp_pop_rua,tp_pop_saude,st_agravo_aids,st_agravo_alcolismo,st_agravo_diabete,
    st_agravo_drogas,st_agravo_mental,st_agravo_outro,st_agravo_tabaco,tp_antirretroviral_trat,tp_sensibilidade,tp_tratamento_acompanhamento) 
    values('%s',nullif('%s','')::timestamp,'%s','%s','%s','%s','%s','%s',nullif('%s','')::timestamp,'%s',nullif('%s','')::timestamp,$$%s$$,$$%s$$,nullif('%s','')::timestamp,
    '%s','%s','%s','%s',$$%s$$,$$%s$$,$$%s$$,nullif('%s','')::numeric,'%s','%s','%s','%s',nullif('%s','')::timestamp,nullif('%s','')::numeric,nullif('%s','')::numeric,'%s','%s',nullif('%s','')::timestamp,
    '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',$$%s$$);
    """ % (df_bd_sinan['nu_notificacao'][i], df_bd_sinan['dt_notificacao'][i], df_bd_sinan['co_municipio_notificacao'][i], df_bd_sinan['co_cnes'][i], df_bd_sinan['cnes_atual'][i], 
           df_bd_sinan['ds_estabelecimento'][i], df_bd_sinan['ds_estab_atual'][i], df_bd_sinan['tp_notificacao'][i], df_bd_sinan['dt_diagnostico_sintoma'][i], df_bd_sinan['co_cid'][i], 
           df_bd_sinan['dt_notificacao_atual'][i], df_bd_sinan['nome_pac'][i], df_bd_sinan['nome_mae'][i], df_bd_sinan['dt_nascimento'][i], df_bd_sinan['tp_sexo'][i], df_bd_sinan['nu_cartao_sus'][i], 
           df_bd_sinan['ds_chave_fonetica'][i], df_bd_sinan['co_municipio_residencia'][i], df_bd_sinan['no_logradouro_residencia'][i], df_bd_sinan['nu_residencia'][i], df_bd_sinan['no_bairro_residencia'][i], 
           df_bd_sinan['co_bairro_residencia'][i],df_bd_sinan['nu_cep_residencia'][i], df_bd_sinan['nu_ddd'][i], df_bd_sinan['nu_tel'][i], df_bd_sinan['tp_entrada'][i], df_bd_sinan['dt_inicio_tratamento'][i],
           df_bd_sinan['nu_contato'][i], df_bd_sinan['nu_contato_examinado'][i], df_bd_sinan['tp_hiv'][i], df_bd_sinan['tp_situacao_encerramento'][i], df_bd_sinan['dt_encerramento'][i], 
           df_bd_sinan['tp_transf'][i], df_bd_sinan['tp_forma'][i], df_bd_sinan['tp_extrapulmonar_1'][i], df_bd_sinan['tp_extrapulmonar_2'][i],
           df_bd_sinan['st_baciloscopia_escarro'][i], df_bd_sinan['st_baciloscopia_escarro2'][i], df_bd_sinan['tp_cultura_escarro'][i], df_bd_sinan['tp_cultura_outro'][i], 
           df_bd_sinan['tp_histopatologia'][i], df_bd_sinan['tp_raio_x'][i], df_bd_sinan['tp_molecular'][i], df_bd_sinan['tp_tratamento'][i], df_bd_sinan['st_baciloscopia_1_mes'][i],
           df_bd_sinan['st_baciloscopia_2_mes'][i], df_bd_sinan['st_baciloscopia_3_mes'][i], df_bd_sinan['st_baciloscopia_4_mes'][i], df_bd_sinan['st_baciloscopia_5_mes'][i], 
           df_bd_sinan['st_baciloscopia_6_mes'][i], df_bd_sinan['st_bacil_apos_6_mes'][i], df_bd_sinan['tp_pop_imigrante'][i], df_bd_sinan['tp_pop_liberdade'][i], df_bd_sinan['tp_pop_rua'][i],
           df_bd_sinan['tp_pop_saude'][i], df_bd_sinan['st_agravo_aids'][i], df_bd_sinan['st_agravo_alcolismo'][i], df_bd_sinan['st_agravo_diabete'][i], df_bd_sinan['st_agravo_drogas'][i], 
           df_bd_sinan['st_agravo_mental'][i], df_bd_sinan['st_agravo_outro'][i], df_bd_sinan['st_agravo_tabaco'][i], df_bd_sinan['tp_antirretroviral_trat'][i], df_bd_sinan['tp_sensibilidade'][i],
           df_bd_sinan['tp_tratamento_acompanhamento'][i])
    try:
        curi.execute(sql)
        coni.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        coni.rollback()
        curi.close()
        break
curi.close()   

dth_now = datetime.now()
dth = dth_now.strftime('%d/%m/%Y %H:%M')
print(dth + ' ********** REGISTROS TUBER_SINAN ATUALIZADOS ************')


#sql ativ tuber esus-------------------------------------------------------------------------------
sql = """
select 1 as ativ_cod,'CONSULTA'::text as ativ_ds,fai.co_fat_cidadao_pec, dta.dt_registro as dt_atend, cb.nu_cbo 
from tb_fat_atendimento_individual fai 
join tb_dim_cbo cb on cb.co_seq_dim_cbo = fai.co_dim_cbo_1 
join tb_dim_tempo dta on dta.co_seq_dim_tempo = fai.co_dim_tempo 
where (fai.ds_filtro_cids LIKE ANY (array['%A150%','%A151%','%A152%','%A153%','%A155%','%A157%','%A158%','%A159%']) or (fai.ds_filtro_ciaps like '%A70%' 
or fai.ds_filtro_ciaps like '%ABP017%')) and fai.co_dim_tempo > 20200000 and (cb.nu_cbo like '225%' or cb.nu_cbo like '2235%') 
and fai.co_fat_cidadao_pec is not null;"""

reg = consultar_db(sql)
df_ativ_con = pd.DataFrame(reg, columns=['ativ_cod','ativ_ds','co_fat_cidadao_pec','dt_atend','nu_cbo'])

sql = """
select 2 as ativ_cod,'HIV AVALIADO' as ativ_ds,fai.co_fat_cidadao_pec,dtt.dt_registro as dt_atend, cb.nu_cbo 
from tb_fat_atendimento_individual fai 
join tb_dim_tempo dtt on dtt.co_seq_dim_tempo = fai.co_dim_tempo 
join tb_dim_cbo cb on cb.co_seq_dim_cbo = fai.co_dim_cbo_1 
join (select distinct tfai.co_fat_cidadao_pec 
from tb_fat_atendimento_individual tfai 
where tfai.co_dim_tempo > 20200000 
and (tfai.ds_filtro_cids LIKE ANY (array['%A150%','%A151%','%A152%','%A153%','%A155%','%A157%','%A158%','%A159%']) or (tfai.ds_filtro_ciaps like '%A70%' 
or tfai.ds_filtro_ciaps like '%ABP017%'))) tb on tb.co_fat_cidadao_pec = fai.co_fat_cidadao_pec 
where fai.co_dim_tempo > 20200000 and (fai.ds_filtro_proced_avaliados like '%0214010058%' or fai.ds_filtro_proced_avaliados like '%ABPG024%' or 
fai.ds_filtro_proced_avaliados like '%0202030040%' or fai.ds_filtro_proced_avaliados like '%0202030296%' or fai.ds_filtro_proced_avaliados like '%0202030300%' or 
fai.ds_filtro_proced_avaliados like '%ABEX018%') and fai.co_fat_cidadao_pec is not null;"""

reg = consultar_db(sql)
df_ativ_hiva = pd.DataFrame(reg, columns=['ativ_cod','ativ_ds','co_fat_cidadao_pec','dt_atend','nu_cbo'])


sql = """
select 2 as ativ_cod,'HIV TESTE RAP' as ativ_ds,fap.co_fat_cidadao_pec,dtt.dt_registro as dt_atend, cb.nu_cbo 
from tb_fat_proced_atend fap 
join tb_dim_tempo dtt on dtt.co_seq_dim_tempo = fap.co_dim_tempo 
join tb_dim_cbo cb on cb.co_seq_dim_cbo = fap.co_dim_cbo 
join (select distinct tfai.co_fat_cidadao_pec 
from tb_fat_atendimento_individual tfai 
where tfai.co_dim_tempo > 20200000 
and (tfai.ds_filtro_cids LIKE ANY (array['%A150%','%A151%','%A152%','%A153%','%A155%','%A157%','%A158%','%A159%']) or (tfai.ds_filtro_ciaps like '%A70%' 
or tfai.ds_filtro_ciaps like '%ABP017%'))) tb on tb.co_fat_cidadao_pec = fap.co_fat_cidadao_pec 
where fap.co_dim_tempo > 20200000 and (fap.ds_filtro_procedimento like '%0214010058%' or fap.ds_filtro_procedimento like '%ABPG024%') 
and fap.co_fat_cidadao_pec is not null;"""

reg = consultar_db(sql)
df_ativ_hivr = pd.DataFrame(reg, columns=['ativ_cod','ativ_ds','co_fat_cidadao_pec','dt_atend','nu_cbo'])


sql = """
select 3 as ativ_cod,'BACILOSCOPIA' as ativ_ds, cp.co_seq_fat_cidadao_pec as co_fat_cidadao_pec, date(ex.dt_realizacao) as dt_atend, ''::text as nu_cbo 
from tb_fat_cidadao_pec cp 
join tb_cidadao c on c.co_seq_cidadao = cp.co_cidadao 
join tb_prontuario pt on pt.co_cidadao = c.co_seq_cidadao 
join tb_exame_requisitado ex on ex.co_prontuario = pt.co_seq_prontuario 
join tb_proced p on p.co_seq_proced = ex.co_proced 
where p.co_proced = '0202080064' and ex.dt_realizacao  > '2021-12-31';"""

reg = consultar_db(sql)
df_ativ_bar = pd.DataFrame(reg, columns=['ativ_cod','ativ_ds','co_fat_cidadao_pec','dt_atend','nu_cbo'])


sql = """
select 4 as ativ_cod,'RX TORAX' as ativ_ds,fai.co_fat_cidadao_pec,dtt.dt_registro as dt_atend, cb.nu_cbo 
from tb_fat_atendimento_individual fai 
join tb_dim_tempo dtt on dtt.co_seq_dim_tempo = fai.co_dim_tempo 
join tb_dim_cbo cb on cb.co_seq_dim_cbo = fai.co_dim_cbo_1 
join (select distinct tfai.co_fat_cidadao_pec 
from tb_fat_atendimento_individual tfai 
where tfai.co_dim_tempo > 20200000 
and (tfai.ds_filtro_cids LIKE ANY (array['%A150%','%A151%','%A152%','%A153%','%A155%','%A157%','%A158%','%A159%']) or (tfai.ds_filtro_ciaps like '%A70%' 
or tfai.ds_filtro_ciaps like '%ABP017%'))) tb on tb.co_fat_cidadao_pec = fai.co_fat_cidadao_pec 
where fai.co_dim_tempo > 20200000 and (fai.ds_filtro_proced_avaliados like '%0204030145%' or fai.ds_filtro_proced_avaliados like '%0204030153%' or 
fai.ds_filtro_proced_avaliados like '%0204030161%' or fai.ds_filtro_proced_avaliados like '%0204030170%' or fai.ds_filtro_proced_avaliados like '%0204030129%' or 
fai.ds_filtro_proced_avaliados like '%0204030137%') and fai.co_fat_cidadao_pec is not null;"""

reg = consultar_db(sql)
df_ativ_rx = pd.DataFrame(reg, columns=['ativ_cod','ativ_ds','co_fat_cidadao_pec','dt_atend','nu_cbo'])

df_ativ_tb = pd.concat([df_ativ_con, df_ativ_hiva,df_ativ_hivr,df_ativ_bar,df_ativ_rx], ignore_index = True)


sql = '''delete from tb_tuber_ativ_old'''
executar_db(sql)

dth_now = datetime.now()
dth = dth_now.strftime('%d/%m/%Y %H:%M')
print(dth + ' ********** REGISTROS ATIV_TUBER ANTIGOS DELETADOS ***********')

coni = conecta_itb()
curi = coni.cursor()

for i in df_ativ_tb.index:
    sql = """
    INSERT into tb_tuber_ativ_old (ativ_cod,ativ_ds,co_fat_cidadao_pec,dt_atend,nu_cbo) values('%s','%s','%s','%s','%s');
    """ % (df_ativ_tb['ativ_cod'][i], df_ativ_tb['ativ_ds'][i], df_ativ_tb['co_fat_cidadao_pec'][i], df_ativ_tb['dt_atend'][i], df_ativ_tb['nu_cbo'][i])
    try:
        curi.execute(sql)
        coni.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        coni.rollback()
        curi.close()
        break
curi.close()




dth_now = datetime.now()
dth = dth_now.strftime('%d/%m/%Y %H:%M')
print(dth + ' ********** REGISTROS ATIV_TUBER ATUALIZADOS ************')

#print(df_ativ_tb)

#tb_alert--------------------------------------------------------------------------

sql = """
select te.co_seq_fat_cidadao_pec, '1'::text as cod_alert 
from tb_tuber_esus te 
join tb_tuber_sinan ts on ts.nome_pac = te.no_cidadao and ts.dt_nascimento = te.dt_nasc 
where (current_date - (ts.dt_inicio_tratamento)) <=180 
and ((current_date - (select max(ta.dt_atend) from tb_tuber_ativ ta where ta.co_fat_cidadao_pec = te.co_seq_fat_cidadao_pec and ta.ativ_cod = 1 
and ta.dt_atend >= ts.dt_inicio_tratamento and ta.dt_atend <= ts.dt_inicio_tratamento+180))>=18 and 
(current_date -(select max(ta.dt_atend) from tb_tuber_ativ ta where ta.co_fat_cidadao_pec = te.co_seq_fat_cidadao_pec and ta.ativ_cod = 1 
and ta.dt_atend >= ts.dt_inicio_tratamento and ta.dt_atend <= ts.dt_inicio_tratamento+180))<=30) 
and ts.dt_encerramento is null;"""

reg = consultar_itb(sql)
df_alert_con = pd.DataFrame(reg, columns=['co_seq_fat_cidadao_pec','cod_alert'])

sql = """
select te.co_seq_fat_cidadao_pec, '2'::text as cod_alert 
from tb_tuber_esus te 
join tb_tuber_sinan ts on ts.nome_pac = te.no_cidadao and ts.dt_nascimento = te.dt_nasc 
where (current_date - (ts.dt_inicio_tratamento)) <=180 and ts.dt_encerramento is null and 
(select count(ta.dt_atend) from tb_tuber_ativ ta where ta.co_fat_cidadao_pec = te.co_seq_fat_cidadao_pec and ta.ativ_cod = 2 
and ta.dt_atend >= ts.dt_inicio_tratamento and ta.dt_atend <= ts.dt_inicio_tratamento+180) <1;"""

reg = consultar_itb(sql)
df_alert_hiv = pd.DataFrame(reg, columns=['co_seq_fat_cidadao_pec','cod_alert'])

sql = """
select te.co_seq_fat_cidadao_pec, '3'::text as cod_alert 
from tb_tuber_esus te 
join tb_tuber_sinan ts on ts.nome_pac = te.no_cidadao and ts.dt_nascimento = te.dt_nasc 
where (current_date - (ts.dt_inicio_tratamento)) <180 and (ts.dt_inicio_tratamento + (((floor(((current_date - ts.dt_inicio_tratamento)+1)/30)+1)*30) -1)::integer) - current_date <=7 
and current_date -(ts.dt_inicio_tratamento + (((floor(((current_date - ts.dt_inicio_tratamento)+1)/30)+1)*30)-1)::integer)<=7 
and (select count(ta.dt_atend) from tb_tuber_ativ ta where ta.co_fat_cidadao_pec = te.co_seq_fat_cidadao_pec and ta.ativ_cod = 3 
and (ts.dt_inicio_tratamento + (((floor(((current_date - ts.dt_inicio_tratamento)+1)/30)+1)*30)-1)::integer) - ta.dt_atend <=7 
and ta.dt_atend -(ts.dt_inicio_tratamento + (((floor(((current_date - ts.dt_inicio_tratamento)+1)/30)+1)*30)-1)::integer)<=7) < 1 
and ts.dt_encerramento is null;"""

reg = consultar_itb(sql)
df_alert_bar = pd.DataFrame(reg, columns=['co_seq_fat_cidadao_pec','cod_alert'])


df_alert = pd.concat([df_alert_con,df_alert_hiv,df_alert_bar ], ignore_index = True)

sql = '''delete from tb_alert_old'''
executar_db(sql)

dth_now = datetime.now()
dth = dth_now.strftime('%d/%m/%Y %H:%M')
print(dth + ' ********** REGISTROS TB_ALERT ANTIGOS DELETADOS ***********')


coni = conecta_itb()
curi = coni.cursor()

for i in df_alert.index:
    sql = """
    INSERT into tb_alert_old (co_seq_fat_cidadao_pec,cod_alert) values('%s','%s');
    """ % (df_alert['co_seq_fat_cidadao_pec'][i], df_alert['cod_alert'][i])
    try:
        curi.execute(sql)
        coni.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        coni.rollback()
        curi.close()
        break
curi.close()

dth_now = datetime.now()
dth = dth_now.strftime('%d/%m/%Y %H:%M')
print(dth + ' ********** REGISTROS TB_ALERT ATUALIZADOS ************')

#renomear tuber_esus
sql = '''ALTER TABLE tb_tuber_esus RENAME TO tb_tuber_esus_tmp;'''
executar_db(sql)

sql = '''ALTER TABLE tb_tuber_esus_old RENAME TO tb_tuber_esus;'''
executar_db(sql)

sql = '''ALTER TABLE tb_tuber_esus_tmp RENAME TO tb_tuber_esus_old;'''
executar_db(sql)

#renomear tuber_sinan
sql = '''ALTER TABLE tb_tuber_sinan RENAME TO tb_tuber_sinan_tmp;'''
executar_db(sql)

sql = '''ALTER TABLE tb_tuber_sinan_old RENAME TO tb_tuber_sinan;'''
executar_db(sql)

sql = '''ALTER TABLE tb_tuber_sinan_tmp RENAME TO tb_tuber_sinan_old;'''
executar_db(sql)

#renomear tuber_ativ
sql = '''ALTER TABLE tb_tuber_ativ RENAME TO tb_tuber_ativ_tmp;'''
executar_db(sql)

sql = '''ALTER TABLE tb_tuber_ativ_old RENAME TO tb_tuber_ativ;'''
executar_db(sql)

sql = '''ALTER TABLE tb_tuber_ativ_tmp RENAME TO tb_tuber_ativ_old;'''
executar_db(sql)

#renomear tb_alert
sql = '''ALTER TABLE tb_alert RENAME TO tb_alert_tmp;'''
executar_db(sql)

sql = '''ALTER TABLE tb_alert_old RENAME TO tb_alert;'''
executar_db(sql)

sql = '''ALTER TABLE tb_alert_tmp RENAME TO tb_alert_old;'''
executar_db(sql)