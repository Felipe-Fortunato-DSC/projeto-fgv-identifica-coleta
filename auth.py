"""Autenticação simples — usuário e senha fixos.

Os valores podem ser sobrescritos por variáveis de ambiente
(APP_USER_NAME / APP_USER_PASSWORD) para evitar credenciais em código.
"""

import os

USER_NAME = os.environ.get("APP_USER_NAME", "FGV")
USER_PASSWORD = os.environ.get("APP_USER_PASSWORD", "FGV2026@")


def verify_login(nome: str, senha: str) -> dict | None:
    if nome == USER_NAME and senha == USER_PASSWORD:
        return {"nome": USER_NAME}
    return None
