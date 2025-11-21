import sys
import re
from typing import Optional, Tuple, Dict, Any

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import get_as_dataframe

# ----------------------------
# CONFIGURAÇÕES (edite aqui)
# ----------------------------
SERVICE_ACCOUNT_FILE = "service_account.json"  # arquivo JSON da service account
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# URL da planilha (cole a url da planilha que você compartilhou com a service account)
SHEET_URL = "https://docs.google.com/spreadsheets/d/1fMERd5gaU-oUMmI80jZmc_2Cxe5nvrFRibJWVHPfsbA/edit"

# senha simples para a aula (mude se quiser)
SYSTEM_PASSWORD = "boravitoria1899"

# ordem exata das colunas na planilha (NÃO mudar a menos que a planilha seja alterada)
COLUMNS = ["id", "nome", "email", "idade", "curso", "turma"]

# ----------------------------
# AUTENTICAÇÃO
# ----------------------------
def auth() -> gspread.Client:
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    return gspread.authorize(creds)


# ----------------------------
# UTILITÁRIOS (planilha)
# ----------------------------
def load_or_create_header(ws):
    """Garante que o cabeçalho exista e esteja na ordem COLUMNS."""
    valores = ws.get_all_values()
    if not valores:
        ws.append_row(COLUMNS)
        return
    header = valores[0]
    # normalizar espaços e comparar
    header_normalized = [h.strip() for h in header]
    if header_normalized != COLUMNS:
        # substituir a primeira linha pelo header correto
        try:
            ws.delete_rows(1)
        except Exception:
            pass
        ws.insert_row(COLUMNS, 1)


def read_all(ws) -> pd.DataFrame:
    """Retorna um DataFrame com todas as linhas, com colunas na ordem COLUMNS."""
    df = get_as_dataframe(ws, evaluate_formulas=True, header=0)
    if df is None:
        df = pd.DataFrame(columns=COLUMNS)
    df = df.dropna(how="all")
    # limpar colunas "Unnamed"
    df = df.loc[:, ~df.columns.str.contains("Unnamed")]
    # garantir todas as colunas existem
    for c in COLUMNS:
        if c not in df.columns:
            df[c] = ""
    # manter ordem
    df = df[COLUMNS]
    # tentar converter id para int quando possível
    if "id" in df.columns:
        def to_int_safe(v):
            try:
                if pd.isna(v) or v == "":
                    return None
                return int(v)
            except Exception:
                return None
        df["id"] = df["id"].apply(to_int_safe)
    return df


def find_row_by_id(ws, id_value: int) -> Optional[int]:
    """Retorna o número da linha no Sheets (1-indexed) para o id, ou None se não encontrar."""
    # ler coluna A (assumindo id está em A)
    try:
        values = ws.col_values(1)  # inclui header
    except Exception:
        return None
    for idx, val in enumerate(values, start=1):
        try:
            if val is None:
                continue
            if str(int(val)) == str(id_value):
                return idx
        except Exception:
            # comparando string se não for inteiro
            if str(val).strip() == str(id_value).strip():
                return idx
    return None


def find_row_by_name(ws, name: str) -> Optional[int]:
    """Retorna a primeira linha onde a coluna 'nome' (coluna B) coincide (case-insensitive)"""
    # ler coluna B (nome)
    try:
        values = ws.col_values(2)
    except Exception:
        return None
    for idx, val in enumerate(values, start=1):
        if val is None:
            continue
        if str(val).strip().lower() == str(name).strip().lower():
            return idx
    return None


# ----------------------------
# CRUD usando ordem COLUMNS
# ----------------------------
def create(ws, data: Dict[str, Any]):
    """Cria um registro; data não precisa conter id (será gerado)."""
    df = read_all(ws)
    # gerar id: max existente + 1, evitando None
    if df.empty or df["id"].dropna().empty:
        novo_id = 1
    else:
        try:
            novo_id = int(df["id"].dropna().max()) + 1
        except Exception:
            # fallback robusto
            ids = [int(x) for x in df["id"].dropna().astype(int).tolist() if str(x).isdigit()]
            novo_id = max(ids) + 1 if ids else 1

    data_with_id = dict(data)
    data_with_id["id"] = novo_id

    ordered = [data_with_id.get(col, "") for col in COLUMNS]
    ws.append_row(ordered)
    return novo_id


def update(ws, row_number: int, data: Dict[str, Any]):
    """Atualiza a linha `row_number` (1-indexed) com valores ordenados por COLUMNS."""
    ordered = [data.get(col, "") for col in COLUMNS]
    # limitar ao número de colunas
    last_col_letter = chr(ord('A') + len(COLUMNS) - 1)
    rng = f"A{row_number}:{last_col_letter}{row_number}"
    ws.update(rng, [ordered])


def delete(ws, row_number: int):
    """Deleta a linha no Sheets (1-indexed)."""
    ws.delete_rows(row_number)


# ----------------------------
# VALIDAÇÕES
# ----------------------------
def is_valid_email(email: str) -> bool:
    return bool(re.match(r"[^@]+@[^@]+\.[^@]+", email))


def input_nonempty(prompt: str) -> str:
    t = input(prompt).strip()
    while t == "":
        print("Valor não pode ficar vazio.")
        t = input(prompt).strip()
    return t


# ----------------------------
# INTERFACE
# ----------------------------
def login() -> bool:
    print("=== SISTEMA DE MATRÍCULAS ===")
    attempts = 3
    while attempts > 0:
        senha = input("Digite a senha de acesso: ")
        if senha == SYSTEM_PASSWORD:
            print("Login autorizado!\n")
            return True
        attempts -= 1
        print(f"Senha incorreta. Tentativas restantes: {attempts}")
    print("Acesso bloqueado.")
    return False


def menu() -> str:
    print("===== MENU =====")
    print("1 - Matricular (CREATE)")
    print("2 - Visualizar (READ)")
    print("3 - Atualizar (UPDATE)")
    print("4 - Deletar (DELETE)")
    print("5 - Buscar")
    print("6 - Sair")
    print("================")
    return input("Escolha uma opção: ").strip()


# ----------------------------
# APLICAÇÃO PRINCIPAL
# ----------------------------
def main():
    if not login():
        return

    try:
        client = auth()
    except Exception as e:
        print("Erro na autenticação com Google:", e)
        return

    try:
        sheet = client.open_by_url(SHEET_URL)
    except Exception as e:
        print("Erro ao abrir a planilha. Verifique SHEET_URL e permissões:", e)
        return

    ws = sheet.sheet1

    # garantir cabeçalho correto
    try:
        load_or_create_header(ws)
    except Exception as e:
        print("Erro garantindo cabeçalho:", e)
        return

    while True:
        opc = menu()

        # CREATE
        if opc == "1":
            print("\n--- Matricular aluno ---")
            nome = input_nonempty("Nome: ")
            email = input_nonempty("Email: ")
            while not is_valid_email(email):
                print("Email inválido.")
                email = input_nonempty("Email: ")
            idade = input("Idade: ").strip()
            curso = input("Curso: ").strip()
            turma = input("Turma: ").strip()

            data = {
                "nome": nome,
                "email": email,
                "idade": idade,
                "curso": curso,
                "turma": turma
            }
            try:
                novo_id = create(ws, data)
                print(f"✔ Aluno criado com id = {novo_id}\n")
            except Exception as e:
                print("Erro ao criar registro:", e)

        # READ
        elif opc == "2":
            print("\n--- Lista completa ---")
            try:
                df = read_all(ws)
                if df.empty or df.shape[0] == 0:
                    print("Nenhum registro encontrado.\n")
                else:
                    # mostrar com alinhamento
                    print(df.fillna("").to_string(index=False))
                    print()
            except Exception as e:
                print("Erro ao ler dados:", e)

        # UPDATE
        elif opc == "3":
            print("\n--- Atualizar registro ---")
            by = input("Atualizar por [id] ou [name]: ").strip().lower() or "id"
            row = None
            df = read_all(ws)
            if by == "id":
                try:
                    id_val = int(input_nonempty("ID do aluno: "))
                except Exception:
                    print("ID inválido.")
                    continue
                row = find_row_by_id(ws, id_val)
                if row is None:
                    print("Registro não encontrado com esse ID.\n")
                    continue
            else:
                name = input_nonempty("Nome do aluno (busca exata): ")
                row = find_row_by_name(ws, name)
                if row is None:
                    print("Registro não encontrado com esse nome.\n")
                    continue

            # ler dados atuais
            try:
                current = ws.row_values(row)
                # padroniza para o tamanho das colunas
                while len(current) < len(COLUMNS):
                    current.append("")
                cur = dict(zip(COLUMNS, current))
            except Exception as e:
                print("Erro ao ler a linha atual:", e)
                continue

            print("Deixe em branco para manter valor atual.")
            novo_nome = input(f"Nome [{cur.get('nome','')}]: ").strip() or cur.get('nome','')
            novo_email = input(f"Email [{cur.get('email','')}]: ").strip() or cur.get('email','')
            if novo_email and not is_valid_email(novo_email):
                print("Email inválido. Abortando update.")
                continue
            nova_id = cur.get("id", "")
            nova_id_parsed = None
            try:
                nova_id_parsed = int(nova_id)
            except Exception:
                # manter original caso não seja int
                try:
                    nova_id_parsed = int(input(f"ID atual '{nova_id}' não é inteiro. Informe novo ID numérico (ou deixe em branco para manter): ") or nova_id)
                except Exception:
                    nova_id_parsed = nova_id

            nova_id = nova_id_parsed
            nova_id = int(nova_id) if isinstance(nova_id, int) or (isinstance(nova_id, str) and nova_id.isdigit()) else nova_id

            nova_id_final = nova_id
            nova_id_final = int(nova_id_final) if isinstance(nova_id_final, (int,)) or (isinstance(nova_id_final, str) and str(nova_id_final).isdigit()) else nova_id_final

            nova_id_final = nova_id_final

            nova_id_out = nova_id_final

            nova_id_out = int(nova_id_out) if isinstance(nova_id_out, (int,)) or (isinstance(nova_id_out, str) and str(nova_id_out).isdigit()) else nova_id_out

            nova_id_out = nova_id_out

            nova_id_value = nova_id_out

            nova_id_value = int(nova_id_value) if isinstance(nova_id_value, (int,)) or (isinstance(nova_id_value, str) and str(nova_id_value).isdigit()) else nova_id_value

            # continuar capturando os outros campos
            nova_id_value = nova_id_value
            nova_id_value = int(nova_id_value) if isinstance(nova_id_value, (int,)) or (isinstance(nova_id_value, str) and str(nova_id_value).isdigit()) else nova_id_value

            nova_id_value_final = nova_id_value

            idade_nova = input(f"Idade [{cur.get('idade','')}]: ").strip() or cur.get('idade','')
            curso_novo = input(f"Curso [{cur.get('curso','')}]: ").strip() or cur.get('curso','')
            turma_nova = input(f"Turma [{cur.get('turma','')}]: ").strip() or cur.get('turma','')

            data_upd = {
                "id": nova_id_value_final,
                "nome": novo_nome,
                "email": novo_email,
                "idade": idade_nova,
                "curso": curso_novo,
                "turma": turma_nova
            }

            try:
                update(ws, row, data_upd)
                print("✔ Registro atualizado!\n")
            except Exception as e:
                print("Erro ao atualizar:", e)

        # DELETE
        elif opc == "4":
            print("\n--- Deletar registro ---")
            by = input("Deletar por [id] ou [name]: ").strip().lower() or "id"
            if by == "id":
                try:
                    id_del = int(input_nonempty("ID do aluno: "))
                except Exception:
                    print("ID inválido.")
                    continue
                row = find_row_by_id(ws, id_del)
                if row is None:
                    print("Registro não encontrado com esse ID.\n")
                    continue
            else:
                name_del = input_nonempty("Nome do aluno (busca exata): ")
                row = find_row_by_name(ws, name_del)
                if row is None:
                    print("Registro não encontrado com esse nome.\n")
                    continue

            confirm = input(f"Confirma excluir a linha {row}? (s/N): ").strip().lower()
            if confirm != "s":
                print("Operação cancelada.\n")
                continue

            try:
                delete(ws, row)
                print("✔ Registro deletado!\n")
            except Exception as e:
                print("Erro ao deletar:", e)

        # SEARCH
        elif opc == "5":
            print("\n--- Buscar registro ---")
            by = input("Buscar por [id] ou [name]: ").strip().lower() or "id"
            if by == "id":
                try:
                    id_search = int(input_nonempty("ID: "))
                except Exception:
                    print("ID inválido.")
                    continue
                row = find_row_by_id(ws, id_search)
                if row is None:
                    print("Registro não encontrado.\n")
                else:
                    vals = ws.row_values(row)
                    # garantir comprimento
                    while len(vals) < len(COLUMNS):
                        vals.append("")
                    rec = dict(zip(COLUMNS, vals))
                    print("\nRegistro encontrado:")
                    for k in COLUMNS:
                        print(f"{k}: {rec.get(k,'')}")
                    print()
            else:
                name_search = input_nonempty("Nome (busca exata): ")
                row = find_row_by_name(ws, name_search)
                if row is None:
                    print("Registro não encontrado.\n")
                else:
                    vals = ws.row_values(row)
                    while len(vals) < len(COLUMNS):
                        vals.append("")
                    rec = dict(zip(COLUMNS, vals))
                    print("\nRegistro encontrado:")
                    for k in COLUMNS:
                        print(f"{k}: {rec.get(k,'')}")
                    print()

        # EXIT
        elif opc == "6":
            print("Saindo...")
            break

        else:
            print("Opção inválida. Tente novamente.\n")


if __name__ == "__main__":
    main()
