import oracledb
import pandas as pd

def conecta_BD():

    try:
        #conectar com o Servidor
        dnStr = oracledb.makedsn("oracle.fiap.com.br","1521","ORCL")
        #efetuar a conexao com o usuario
        conect = oracledb.connect(user='RM553799',password='fiap24',dsn=dnStr)

        instrucao_SQL = conect.cursor()

    except Exception as e:
        print("Erro: ", e)
        conexao = False
        instrucao_SQL = ""
        conn = ""
    else:
        conexao = True

    return(conexao,instrucao_SQL,conect)
def criar_conta():
    conexao, instrucao_SQL, conect = conecta_BD()

    try:
        id_clie = int(input("Digite o id do cliente: "))
        nm_clie = input("Digite o nome do cliente: ")
        email_clie = input("Digite o email do cliente: ")
        telefone = int(input("Digite o telefone do cliente: "))
        tm_empresa = int(input("Digite o tamanho da empresa: "))

        id_usuario = int(input("Digite o id do usuario: "))
        nm_usuario = input("Digite o nome do usuario: ")
        senha_usuario = input("Digite o senha do usuario: ")

        insercao_usuario = f"""INSERT INTO USUARIO (id_usuario,nm_usuario,senha_usuario) 
                                VALUES ({id_usuario}, '{nm_usuario}', '{senha_usuario}')"""

        instrucao_SQL.execute(insercao_usuario)
        conect.commit()

        insercao = f"""INSERT INTO CLIENTE (id_clie,telefone,tm_empresa,email_clie,nm_clie,fk_usuario) 
                        VALUES ({id_clie}, '{telefone}', '{tm_empresa}', '{email_clie}', '{nm_clie}', {id_usuario})"""

        instrucao_SQL.execute(insercao)
        conect.commit()
    except ValueError:
        print("Digite dados numéricos")
    except Exception as erro:
        print("Erro: ", erro)
    else:
        print("Dados gravados com sucesso!")
        print("\n")

def editar_conta():
    conexao, instrucao_SQL, conect = conecta_BD()

    id = int(input("Digite o id do cliente a ser alterado: "))
    lista_dados = []

    str_consulta = f"""SELECT * FROM CLIENTE where ID_CLIE = {id}"""

    instrucao_SQL.execute(str_consulta)

    # capturar todos os registros da consulta
    dados = instrucao_SQL.fetchall()

    for dado in dados:
        lista_dados.append(dado)

    if (len(lista_dados) == 0):
        print("O id a ser alterado não existe!")
    else:
        try:
            nm_clie = input("Digite o nome do cliente: ")
            email_clie = input("Digite o email do cliente: ")
            telefone = int(input("Digite o telefone do cliente: "))
            tm_empresa = int(input("Digite o tamanho da empresa: "))

            str_alteracao = f"""UPDATE cliente SET nm_clie='{nm_clie}',email_clie='{email_clie}',telefone={telefone},tm_empresa={tm_empresa} WHERE id_clie={id}"""
            instrucao_SQL.execute(str_alteracao)
            conect.commit()
        except ValueError:
            print("Digite dados numéricos")
        except Exception as erro:
            print("Erro: ", erro)
        else:
            print("Dados alterados com sucesso")
            print("\n")

def excluir_conta():
    conexao, instrucao_SQL, conect = conecta_BD()

    id = int(input("Digite o id do cliente a ser excluido: "))

    lista_dados = []

    str_consulta = f"""SELECT * FROM cliente where id_clie = {id}"""

    instrucao_SQL.execute(str_consulta)

    # capturar todos os registros da consulta
    dados = instrucao_SQL.fetchall()

    for dado in dados:
        lista_dados.append(dado)

    if (len(lista_dados) == 0):
        print("O id do cliente a ser excluido não existe!")
    else:
        try:
            str_exclusao = f"""DELETE FROM cliente where id_clie={id}"""
            instrucao_SQL.execute(str_exclusao)
            conect.commit()
        except Exception as erro:
            print("Erro: ", erro)
        else:
            print("Cliente excluído com sucesso")
            print("\n")

def menu_crud():
    while True:
        print("\nMenu CRUD de Contas\n1- Adicionar Conta\n2- Editar Conta\n3- Excluir Conta\n4- Listar Contas\n0- Sair")
        escolha = input("Escolha uma opção do menu acima: ")

        if escolha == "1":
            criar_conta()
        elif escolha == "2":
            editar_conta()
        elif escolha == "3":
            excluir_conta()
        elif escolha == "4":
            exibir_contas()
        elif escolha == "0":
            break
        else:
            print("Opção inválida. Por favor, escolha uma opção válida.")

def exibir_contas():
    conexao, instrucao_SQL, conect = conecta_BD()
    lista_dados = []

    instrucao_SQL.execute("SELECT * FROM CLIENTE")

    dados = instrucao_SQL.fetchall()

    for dado in dados:
        lista_dados.append(dado)

    lista_dados = sorted(lista_dados)

    dados_df = pd.DataFrame.from_records(lista_dados, columns=['ID_CLIE', 'TELEFONE', 'TM_EMPRESA', 'EMAIL_CLIE', 'NM_CLIE', 'FK_USUARIO'], index='ID_CLIE')

    if (dados_df.empty):
        print("Não há registros na tabela")
    else:
        print(dados_df)
        print("\n")

def consultar_nome_cliente():
    conexao, instrucao_SQL, conect = conecta_BD()
    lista_dados = []

    nome = input("Digite nome do cliente que deseja consultar: ")

    instrucao_SQL.execute(f"SELECT * FROM cliente WHERE nm_clie = '{nome}'")

    # capturar todos os registros da consulta
    dados = instrucao_SQL.fetchall()

    for dado in dados:
        lista_dados.append(dado)

    lista_dados = sorted(lista_dados)

    dados_df = pd.DataFrame.from_records(lista_dados, columns=['ID_CLIE', 'TELEFONE', 'TM_EMPRESA', 'EMAIL_CLIE', 'NM_CLIE', 'FK_USUARIO'], index='ID_CLIE')

    if (dados_df.empty):
        print("Não há registros na tabela com clientes com esse nome")
    else:
        print(dados_df)
        print("\n")

    escolha = "z"
    while escolha not in ['S', 'N', 's', 'n']:
        escolha = input("\nDeseja salvar os dados em json? [S/N]: ")
        if escolha == "S" or escolha == "s":
            nome_arquivo = "nome_clientes.json"
            dados_df.to_json(nome_arquivo, indent=4, orient='records')

            print("Arquivo salvo com sucesso!")
        elif escolha != "S" and escolha != "N" and escolha != "n" and escolha != "s":
            print("Opção invalida!")

def consultar_tamanho_empresa():
    conexao, instrucao_SQL, conect = conecta_BD()
    lista_dados = []

    tamanho = input("Digite o valor a partir de qual tamanho de empresa que deseja consultar: ")

    instrucao_SQL.execute(f"SELECT * FROM cliente WHERE tm_empresa >= {tamanho}")

    # capturar todos os registros da consulta
    dados = instrucao_SQL.fetchall()

    for dado in dados:
        lista_dados.append(dado)

    lista_dados = sorted(lista_dados)

    dados_df = pd.DataFrame.from_records(lista_dados, columns=['ID_CLIE', 'TELEFONE', 'TM_EMPRESA', 'EMAIL_CLIE', 'NM_CLIE', 'FK_USUARIO'], index='ID_CLIE')

    if (dados_df.empty):
        print("Não há registros na tabela com alunos maiores de 20 anos")
    else:
        print(dados_df)
        print("\n")

    escolha = "z"
    while escolha not in ['S', 'N', 's', 'n']:
        escolha = input("\nDeseja salvar os dados em json? [S/N]: ")
        if escolha == "S" or escolha == "s":
            nome_arquivo = "tamanho_empresas.json"
            dados_df.to_json(nome_arquivo, indent=4, orient='records')

            print("Arquivo salvo com sucesso!")
        elif escolha != "S" and escolha != "N" and escolha != "n" and escolha != "s":
            print("Opção invalida!")

def main():
    conexao, instrucao_SQL, conect = conecta_BD()

    opcao = "1"
    while opcao == "1" and conexao==True:

        print("\nMenu Salesforce\n1- Contato\n2- Novos Produtos\n3- Chatbot\n4- Cadastro\n5- CRUD de Contas\n6- Consultar tamanho das empresas\n7- Consultar clientes pelo nome\n0- Sair")
        escolha = input("Escolha uma opção do menu acima: ")

        match escolha:
            case "1":
                print("\nFalar com Bruno, Chrisman ou Leonardo da turma 1TDSPR")
            case "2":
                print("\nNesta sessão estará todos os novos produtos que a Salesforce estiver anunciando, como promoções, etc.")
            case "3":
                print("\nChatbot: tirar as dúvidas dos clientes, trazendo mais agilidade no suporte ao cliente. Esse Chatbot contará com um beta de 3 dos suas mascotes programadas em 'Watson Assistant' para resolver alguns problemas simples e tirar dúvidas frequentes de determinada área em que o mascote 'atua'.")
            case "4":
                criar_conta()
            case "5":
                menu_crud()
            case "6":
                consultar_tamanho_empresa()
            case "7":
                consultar_nome_cliente()
            case "0":
                break
            case _:
                print("Opção inválida. Escolha uma opção válida.")

        opcao = input("\nDeseja continuar? (1-SIM 0-NÃO): ")


if __name__ == "__main__":
    main()
