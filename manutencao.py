import conexao as cnx

cur_orig = cnx.cur
cur_dest = cnx.cur_d

def cria_campo(nome_tabela, nome_campo):
    resultado = cur_dest.execute(
        "select count(*) from rdb$relation_fields where rdb$relation_name = '{tabela}' and(rdb$field_name = '{campo}')".format(
            tabela=nome_tabela.upper(), campo=nome_campo.upper())).fetchone()[0]

    if resultado == 0:
        cur_dest.execute("alter table {tabela} add {campo} varchar(20)".format(
            tabela=nome_tabela, campo=nome_campo))
cnx.commit()

def empresa():
    return cur_dest.execute("select empresa from cadcli").fetchone()[0]


