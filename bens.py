import conexao as cnx
import manutencao

cur_orig = cnx.cur
cur_dest = cnx.cur_d

empresa = manutencao.empresa()

def cadastro():
    print("Inserindo bens do Patrimonio")

    cur_dest.execute("update cadcli set bloqueiop = null")
    cur_dest.execute("delete from pt_movbem")
    cur_dest.execute("delete from pt_cadpat")

    cur_orig.execute("""
        select
            case when cast(b.codigo_tp_bem as integer) = 501 or cast(b.codigo_tp_bem as integer) = 502 then
            cast(substring(b.codigo_bem ,4,1000) as integer)
        	else cast(b.codigo_bem as integer) end as placa,
            cast(b.codigo_bem as integer) as codigo,
            cast(b.codigo_tp_bem as integer) as grupo,
            coalesce(b.especificacao, 'Não Definido') as especificacao,
            coalesce(b.descricao , 'Não Definido') as descricao,
            case
                b.codigo_tp_aquisi when '701' then 'C'
                when '702' then 'D'
                when '703' then 'D'
                when '705' then 'O'
                else 'I'
            end as compra,
            b.data_aquisi as data_aquisicao ,
            cast(b.codigo_classe as integer) as tipo,
            b.vlr_aquisi as valor_aquisicao ,
            b.vlr_atual as valor_atual,
            cast(b.codigo_tp_conserva_bem as integer) as conservacao,
            cast(b.codigo_local_aquisi as integer) as local_aquisicao ,
            cast(b.codigo_local_atual as integer) as local_atual,
            f.seq_gg_pessoa as fornecedor,
            b.data_ult_local as data_local,
            b.vida_util ,
            b.valor_residual ,
            substring(c.conta_contabil,1,9) as conta_contabil,
            g.nome_g as responsavel
        from
            pt_bens b
        left join pt_classes c on
            c.codigo_emp = b.codigo_emp
            and c.codigo_fil = b.codigo_fil
            and c.codigo_classe = b.codigo_classe
        left join gg_geral g on
            g.codigo_emp = b.codigo_emp
            and g.codigo_g = b.codigo_g_responsavel
        left join gg_geral f on  f.codigo_emp = b.codigo_emp
            and f.codigo_g = b.codigo_g_fornecedor        
        """
                   )

    insert_cadastro = cur_dest.prep(
        """
        insert
        into
        pt_cadpat(
            codigo_pat
            , empresa_pat
            , codigo_gru_pat
            , chapa_pat
            , codigo_set_pat
            , codigo_set_atu_pat            
            , orig_pat
            , obs_pat
            , codigo_sit_pat
            , discr_pat
            , datae_pat
            , dtlan_pat
            , dt_contabil
            , valaqu_pat
            , valatu_pat
            , valres_pat
            , dae_pat
            , percentemp_pat
            , percenqtd_pat
            , codigo_cpl_pat
            , codigo_tip_pat
            , codigo_for_pat
            , responsa_pat
            )
        values( ?,? ,? ,? ,?
            ,? ,? ,? ,? ,? ,? 
            ,? ,? ,? ,? ,? ,?
            ,? ,? ,? ,? ,? ,?)
        """
    )

    insert_movimento = cur_dest.prep(
        """
        insert
        into
        pt_movbem(
            codigo_mov
            , empresa_mov
            , codigo_pat_mov
            , data_mov
            , tipo_mov
            , valor_mov
            , lote_mov
            , codigo_cpl_mov
            , codigo_set_mov
            , documento_mov
            , dt_contabil
            , depreciacao_mov
            , codigo_bai_mov
        )
        values(?,?,?,?,?
        ,?,?,?,?,?,?,?,?)
        """
    )

    sequencia_mov = 0

    for row in cur_orig:
        descricao = row.descricao.encode("cp1252", errors='replace').decode('cp1252')
        

        cur_dest.execute(insert_cadastro, (row.codigo, empresa, row.grupo, str(row.placa).zfill(6), row.local_aquisicao, row.local_atual,
                                         row.compra, descricao.title(), row.conservacao, row.especificacao.title(),
                                         row.data_aquisicao, row.data_aquisicao, row.data_aquisicao, row.valor_aquisicao, row.valor_atual,
                                         row.valor_residual, 'V', 'M', int(row.vida_util) * 12, row.conta_contabil, row.tipo,  row.fornecedor,
                                         row.responsavel))

        sequencia_mov += 1

        cur_dest.execute(insert_movimento, (sequencia_mov, empresa, row.codigo, row.data_aquisicao,
                                          'A', row.valor_aquisicao, None, row.conta_contabil, row.local_aquisicao, row.especificacao[:30].title(),
                                          row.data_aquisicao, 'N', None))

        if row.local_atual != row.local_aquisicao:
            sequencia_mov += 1

            cur_dest.execute(insert_movimento, (sequencia_mov, empresa, row.codigo, row.data_local, 'T', 0,
                                              None, row.conta_contabil, row.local_atual, 'Transfêrencia',
                                              row.data_local, 'N', None))
    cnx.commit()

def mov_baixas():
    print("Inserindo movimentação de baixa do Patrimônio")

    cur_orig.execute(
        """
        select
            cast(i.codigo_bem as integer) as codigo,
            cast(i.codigo_mot as integer) as codigo_baixa,
            i.vlr_baixado *-1 as valor,
            n.descricao_baixa as descricao,
            n.data_baixa
        from
            pt_baixa_item i
        left join pt_baixa_nota n on
            n.codigo_emp = i.codigo_emp
            and n.codigo_fil = i.codigo_fil
            and n.num_guia = i.num_guia        
        """
    )

    insert = cur_dest.prep(
        """
        insert
        into
        pt_movbem(
            codigo_mov
            , empresa_mov
            , codigo_pat_mov
            , data_mov
            , tipo_mov
            , valor_mov
            , lote_mov
            , codigo_cpl_mov
            , codigo_set_mov
            , documento_mov
            , dt_contabil
            , depreciacao_mov
            , codigo_bai_mov
        )
        values(?,?,?,?,?
        ,?,?,?,?,?,?,?,?)
        """
    )

    sequencia = int(cur_dest.execute(
        "select coalesce(max(codigo_mov),0) from pt_movbem").fetchone()[0])

    for row in cur_orig:
        sequencia += 1

        cur_dest.execute(insert, (sequencia, empresa, row.codigo, row.data_baixa,
                                  'B', row.valor, None, None, None, row.descricao[:30] if row.descricao != None else None, 
                                  row.data_baixa, 'N', row.codigo_baixa))
    cnx.commit()

    cur_dest.execute(
        """
        update
            pt_movbem m
        set
            m.valor_mov = (
            select
                sum(t.valor_mov) *-1
            from
                pt_movbem t
            where
                t.codigo_pat_mov = m.codigo_pat_mov
                and t.tipo_mov <> 'B')
        where
            m.tipo_mov = 'B'
            and m.valor_mov = 0
        """)
    cnx.commit()

    cur_dest.execute(
        "update pt_cadpat p set p.valatu_pat = (select sum(m.valor_mov) from pt_movbem m where m.codigo_pat_mov = p.codigo_pat )")
    cnx.commit()

    cur_dest.execute("""
        update
            pt_cadpat p
        set
            p.dtpag_pat = (
                select
                    m.data_mov
                from
                    pt_movbem m
                where
                    m.codigo_pat_mov = p.codigo_pat
                    and m.tipo_mov = 'B'
            )
        where
            exists (
                select
                    1
                from
                    pt_movbem b
                where
                    b.codigo_pat_mov = p.codigo_pat
                    and b.tipo_mov = 'B'
            )"""
                   )
    cnx.commit()

    cur_dest.execute("""
        update
            pt_cadpat p
        set
            p.codigo_bai_pat = (
                select
                    m.codigo_bai_mov
                from
                    pt_movbem m
                where
                    m.codigo_pat_mov = p.codigo_pat
                    and m.tipo_mov = 'B'
            )
        where
            exists (
                select
                    1
                from
                    pt_movbem b
                where
                    b.codigo_pat_mov = p.codigo_pat
                    and b.tipo_mov = 'B'
            )"""
                   )

    cnx.commit()

    cur_dest.execute(
        """update parampatri set correlacao_pcasp_ok  = 'S', exer_enc  = null""")
    cnx.commit()

    cur_dest.execute(
        """
        update
            pt_movbem m
        set
            m.codigo_cpl_mov = (
            select
                p.codigo_cpl_pat
            from
                pt_cadpat p
            where
                p.codigo_pat = m.codigo_pat_mov )
        where
            m.codigo_cpl_mov is null
        """
    )
    cnx.commit()