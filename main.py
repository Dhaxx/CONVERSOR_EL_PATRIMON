import cadastros
import bens

def main():
    cadastros.limpa_tabelas()
    cadastros.tipos()
    cadastros.grupos()
    cadastros.situacoes()
    cadastros.secretaria()
    cadastros.local()
    cadastros.baixas()

    bens.cadastro()
    bens.mov_baixas()

if __name__ == "__main__":
    main()