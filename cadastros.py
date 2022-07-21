import conexao as cnx
import manutencao

cur_orig = cnx.cur
cur_dest = cnx.cur_d

def limpa_tabelas():
    tables = [        
        "pt_movbem",
        "pt_cadpat",
        "pt_cadtip",
        "pt_cadpatg",
        "pt_cadsit",
        "pt_cadpats",
        "pt_cadpatd",
        "pt_cadbai"]

    for table in tables:
        cur_dest.execute(f"delete from {table}")
    cnx.commit()

def tipos():
    print("Tipo de Bens do Patrimonio")

    cur_orig.execute("""select codigo_classe as tipo, nome_classe as nome from pt_classes""")

    insert = cur_dest.prep("insert into pt_cadtip(codigo_tip, empresa_tip, descricao_tip) values(?,?,?)")

    for row in cur_orig:
        cur_dest.execute(insert,(row.tipo, manutencao.empresa(), row.nome[:60].title()))
    cnx.commit()

def grupos():
    print("Grupos do Patrimonio...")

    cur_orig.execute(
        "select codigo_tp_bem as grupo , nome_tp_bem as nome from pt_tp_bens")

    insert = cur_dest.prep(
        "insert into pt_cadpatg(codigo_gru,empresa_gru,nogru_gru) values(?,?,?)")

    for row in cur_orig:
        cur_dest.execute(insert,(row.grupo, manutencao.empresa(), row.nome[:60].title()))
    cnx.commit()

def situacoes():
    print("Situação do Patrimonio")

    cur_orig.execute(
        "select codigo_tp_conserva as situacao, nome_tp_conserva as nome from  pt_tp_conserva")

    insert = cur_dest.prep(
        "insert into pt_cadsit(codigo_sit, empresa_sit, descricao_sit) values (?,?,?)")

    for row in cur_orig:
        cur_dest.execute(insert, (row.situacao, manutencao.empresa(), row.nome.title()))
    cnx.commit()

def secretaria():
    print("Unidade do Patrimonio")

    cur_orig.execute(
        """
        select
            distinct cast(gs.codigo_secre as integer) as codigo,
            coalesce(gs.nome_secre , 'Não Definido') as nome ,
            coalesce(gg.nome_g, 'Não Definido') as responsavel
        from
            gg_local l
        inner join gg_secre gs on
            gs.codigo_secre = l.codigo_secre
        left join gg_geral gg on
            gg.codigo_emp = l.codigo_emp
            and gg.codigo_g = gs.codigo_g_responsavel 
        where cast(gs.codigo_secre as integer) > 0     
        """
    )

    insert = cur_dest.prep(
        "insert into pt_cadpatd(codigo_des,empresa_des,nauni_des, responsa_des) values (?,?,?,?)")

    for row in cur_orig:
        cur_dest.execute(insert, (row.codigo, manutencao.empresa(), row.nome.title(), row.responsavel.title()))
    
    cur_dest.execute(insert, (0,manutencao.empresa(), 'Sem Secretaria', None))

    cnx.commit()


def local():
    print("Localização do Patrimonio")

    cur_orig.execute(
        """
        select
            l.codigo_local as codigo,
            coalesce(cast(l.codigo_secre as integer),0) as unidade,
            coalesce(l.nome_local,'Não Definido') as nome ,
            coalesce(gg.nome_g,'Não Definido') as responsavel
        from
            gg_local l
        left join gg_geral gg on
            gg.codigo_emp = l.codigo_emp
            and gg.codigo_g = l.codigo_g_responsavel        
        """
    )

    insert = cur_dest.prep(
        "insert into pt_cadpats(codigo_set,empresa_set,codigo_des_set, noset_set, responsa_set) values (?,?,?,?,?)")

    for row in cur_orig:
        cur_dest.execute(insert, (row.codigo,manutencao.empresa(), row.unidade, row.nome.title(), row.responsavel.title()))
    cnx.commit()


def baixas():
    print("Tipos de baixas no Patrimonio")
    cur_orig.execute(
        "select codigo_mot as codigo, nome_mot as nome from pt_motivos")

    insert = cur_dest.prep(
        "insert into pt_cadbai(codigo_bai, empresa_bai, descricao_bai) values (?,?,?)")

    for row in cur_orig:
        cur_dest.execute(insert, (row.codigo, manutencao.empresa(), row.nome.title()))
    cnx.commit()
