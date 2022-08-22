import pyodbc
import pandas as pd
import keyboard

#DEFINICAO DOS DADOS DO BANCO DE DADOS DO METADADOS SIRH
server = 'tcp:bancodedados.endereco.com.br' 
database = 'nomeDoBanco' 
username = 'usuarioBanco' 
password = 'senhaBanco'

#DEFINICAO DA STRING DE CONEXAO DO BANCO DE DADOS
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)

#QUERIE DE CONSULTA DOS FUNCIONARIOS COM CONTRATO ATIVO NO SIRH
querieUsuarios = "SELECT C.CONTRATO as id, C.CONTRATO as registration, PE.NOME as name,'' as cpf,'' as phone,'' as cargo,'' as password,'' as pis,'' as barras,'' as rg,'' as telefone,'' as email,'' as emailAcesso,'' as endereco,'' as bairro,'' as cidade,'' as cep,'' as ramal,'' as pai,'' as mae,'' as sexo,'' as admissao,'' as nascimento,'' as estadoCivil,'' as nacionalidade,'' as naturalidade,'' as dateStartLimit,'' as dateLimit,'' as comments,'Não' as inativo,'Não' as contingency,'Não' as blackList,'' as cards,'' as templates,'' as idDevice,'1' as idDepartamentos from dbo.RHCONTRATOS as C JOIN dbo.RHPESSOAS AS PE ON PE.PESSOA = C.PESSOA JOIN dbo.RHCARGOS AS CA ON CA.CARGO = C.CARGO JOIN dbo.RHSETORES AS SE ON SE.SETOR = C.SETOR JOIN dbo.RHFUNCOES AS FU ON FU.FUNCAO = C.FUNCAO JOIN dbo.RHEMPRESAS AS EM ON EM.EMPRESA = C.EMPRESA WHERE CONTRATOSATIVOS = 1 and CAUSARESCISAOFOLHA is NULL ORDER BY PE.NOME"
dfUsuarios = pd.read_sql(querieUsuarios, cnxn)

#QUERIE DE CONSULTA DOS TEMPLATES DE DIGITAIS DOS FUNCIONARIOS
querieDigitais = "SELECT D.[EMPRESA] ,C.[CONTRATO] ,D.[PESSOA] ,PE.[NOME] ,D.[MAO] ,D.[DEDO] ,D.[DIGITALNOVO] ,D.[TEMPLATE] FROM [metadados].[dbo].[RHPESSOASDIGITAISIDBIO] AS D JOIN dbo.RHPESSOAS AS PE ON PE.PESSOA = D.PESSOA JOIN dbo.RHCONTRATOS as C ON PE.PESSOA = C.PESSOA WHERE CONTRATOSATIVOS = 1 and CAUSARESCISAOFOLHA is NULL ORDER BY PE.NOME"
dfDigitais = pd.read_sql(querieDigitais, cnxn)

#CONSULTA TEMPLATES DE DIGITAIS DE CADA USUARIO E INSERE NO CAMPO TEMPLATE DO DATAFRAME DFUSUARIOS
for indexUsuarios, rowUsuarios in dfUsuarios.iterrows():
    digitais = ''
    for indexDigitais, rowDigitais in dfDigitais.iterrows():
        if(rowUsuarios['id']==rowDigitais['CONTRATO']):
            if(digitais!=''):
                digitais = digitais+'|'
            digitais = digitais + str(rowDigitais['TEMPLATE']) + '|0'
    if(digitais!=''):
        dfUsuarios.at[indexUsuarios, 'templates'] = digitais

#CRIA CSV COM O DATAFRAME DFUSUARIOS NO FORMATO PARA IMPORTACAO NO IDSECURE DA CONTROLID
#NO LUGAR DO NOME DO ARQUIVO PODE USAR UM CAMINHO DE REDE CONFORME O EXEMPLO: "\\\\fileserver.dominio.com.br\\Sistemas\\Integrações\\Metadados\\metadadosUsuariosControlID.csv"
dfUsuarios.to_csv('metadadosUsuariosControlID.csv',sep=',', index=False, header=True)

#SINALIZA QUE FINALIZOU
print('FINALIZOU')
keyboard.wait("esc")
