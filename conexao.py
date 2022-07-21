import fdb 
import pyodbc

conexao_destino = fdb.connect(dsn="localhost:D:\Fiorilli\SCPI_8\Cidades\AVARE - PM\ARQ2022\SCPI2022.FDB", user='fscscpi8',
                     password='scpi', port=3050, charset='WIN1252')

conexao_origem = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;PORT=1433;DATABASE=PM_Avare;UID=Sa;PWD=Dnal250304')

cur = conexao_origem.cursor()

cur_d = conexao_destino.cursor()

def commit():
    conexao_destino.commit()

def get_cursor(conexao):
    return conexao.cursor()