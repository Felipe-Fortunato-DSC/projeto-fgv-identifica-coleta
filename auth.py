"""
Módulo de autenticação — gerenciamento de usuários e sessão.
Armazena os usuários na mesma base SQLite do projeto.
"""

import hashlib
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path

DB_PATH = Path(__file__).parent / "data" / "database.db"

_MASTER_NOME = "Master"
_MASTER_SENHA = "Master290915@"


# ---------------------------------------------------------------------------
# Helpers de senha
# ---------------------------------------------------------------------------

def _hash_password(password: str, salt: str | None = None) -> tuple[str, str]:
    if salt is None:
        salt = os.urandom(16).hex()
    key = hashlib.pbkdf2_hmac(
        "sha256", password.encode("utf-8"), salt.encode("utf-8"), 100_000
    )
    return key.hex(), salt


def _verify_password(password: str, stored_hash: str, salt: str) -> bool:
    computed, _ = _hash_password(password, salt)
    return computed == stored_hash


# ---------------------------------------------------------------------------
# Conexão
# ---------------------------------------------------------------------------

@contextmanager
def _get_conn():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Inicialização
# ---------------------------------------------------------------------------

def init_auth() -> None:
    """Cria a tabela de usuários e garante a existência do usuário Master."""
    with _get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS usuarios (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                nome        TEXT    NOT NULL UNIQUE,
                email       TEXT,
                senha_hash  TEXT    NOT NULL,
                salt        TEXT    NOT NULL,
                tipo        TEXT    NOT NULL
                            CHECK(tipo IN ('master','administrador','tecnico')),
                area        TEXT,
                ativo       INTEGER NOT NULL DEFAULT 1
            )
        """)
        # Migração: adiciona coluna area se não existir
        try:
            conn.execute("ALTER TABLE usuarios ADD COLUMN area TEXT")
        except Exception:
            pass
        if not conn.execute(
            "SELECT 1 FROM usuarios WHERE tipo='master'"
        ).fetchone():
            h, s = _hash_password(_MASTER_SENHA)
            conn.execute(
                "INSERT INTO usuarios (nome, email, senha_hash, salt, tipo) "
                "VALUES (?,?,?,?,?)",
                (_MASTER_NOME, None, h, s, "master"),
            )
        conn.commit()


# ---------------------------------------------------------------------------
# Operações públicas
# ---------------------------------------------------------------------------

def verify_login(nome: str, password: str) -> dict | None:
    """Verifica credenciais. Retorna dict do usuário ou None."""
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM usuarios WHERE nome=? AND ativo=1", (nome,)
        ).fetchone()
    if row and _verify_password(password, row["senha_hash"], row["salt"]):
        return dict(row)
    return None


def get_all_users() -> list[dict]:
    with _get_conn() as conn:
        rows = conn.execute(
            "SELECT id, nome, email, tipo, area, ativo FROM usuarios ORDER BY tipo, nome"
        ).fetchall()
    return [dict(r) for r in rows]


def create_user(nome: str, email: str, senha: str, tipo: str, area: str = "") -> tuple[bool, str]:
    nome = nome.strip()
    email = email.strip()
    area = area.strip()
    if not nome:
        return False, "Nome é obrigatório."
    if tipo in ("administrador", "tecnico") and not email:
        return False, "Email é obrigatório para Administrador e Técnico."
    if not senha:
        return False, "Senha é obrigatória."
    if len(senha) < 6:
        return False, "A senha deve ter pelo menos 6 caracteres."
    with _get_conn() as conn:
        if conn.execute(
            "SELECT 1 FROM usuarios WHERE nome=?", (nome,)
        ).fetchone():
            return False, f"Já existe um usuário chamado '{nome}'."
        h, s = _hash_password(senha)
        conn.execute(
            "INSERT INTO usuarios (nome, email, senha_hash, salt, tipo, area) VALUES (?,?,?,?,?,?)",
            (nome, email or None, h, s, tipo, area or None),
        )
        conn.commit()
    return True, "Usuário criado com sucesso."


def remove_user(user_id: int) -> tuple[bool, str]:
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT tipo FROM usuarios WHERE id=?", (user_id,)
        ).fetchone()
        if not row:
            return False, "Usuário não encontrado."
        if row["tipo"] == "master":
            return False, "O usuário Master não pode ser removido."
        conn.execute("DELETE FROM usuarios WHERE id=?", (user_id,))
        conn.commit()
    return True, "Usuário removido com sucesso."


def change_password(nome: str, senha_atual: str, nova_senha: str) -> tuple[bool, str]:
    if len(nova_senha) < 6:
        return False, "A nova senha deve ter pelo menos 6 caracteres."
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM usuarios WHERE nome=? AND ativo=1", (nome,)
        ).fetchone()
        if not row:
            return False, "Usuário não encontrado."
        if not _verify_password(senha_atual, row["senha_hash"], row["salt"]):
            return False, "Senha atual incorreta."
        h, s = _hash_password(nova_senha)
        conn.execute(
            "UPDATE usuarios SET senha_hash=?, salt=? WHERE id=?",
            (h, s, row["id"]),
        )
        conn.commit()
    return True, "Senha alterada com sucesso."
