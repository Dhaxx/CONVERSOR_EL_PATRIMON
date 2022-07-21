import fdb 
import pyodbc

conexao_destino = fdb.connect(dsn="localhost:F:\Fiorilli\Cidades\Igaracu-PM\ARQ2021\SCPI2021.FDB", user='fscscpi8',
                     password='scpi', port=3050, charset='WIN1252')

conexao_origem = pyodbc.connect('DRIVER={SQL Server};SERVER=localhost;PORT=1433;DATABASE=patrimon;Trusted_Connection=yes')

cur = conexao_origem.cursor()

cur_d = conexao_destino.cursor()

def commit():
    conexao_destino.commit()

def get_cursor(conexao):
    return conexao.cursor()