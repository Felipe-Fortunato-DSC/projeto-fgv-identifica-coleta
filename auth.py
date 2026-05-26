"""Autenticação simples — credenciais por perfil via variáveis de ambiente.

O perfil (ADMIN, DEV, VALIDADOR, ANALISTA) é determinado pelo login utilizado.
Cada perfil tem usuário e senha definidos por variáveis de ambiente:
  APP_<ROLE>_USER     (opcional — default <role>.fgv)
  APP_<ROLE>_PASSWORD (obrigatória — sem ela o perfil fica indisponível)

Nenhuma senha é embutida no código, para não vazar em repositório público.
Defina as senhas no .env (que está no .gitignore) ou na Task Definition.
"""

import os

ROLES = ["ADMIN", "DEV", "VALIDADOR", "ANALISTA"]

# Apenas o nome de login por perfil (não é segredo). A senha vem do ambiente.
_DEFAULT_USERS = {
    "ADMIN":     "admin.fgv",
    "DEV":       "dev.fgv",
    "VALIDADOR": "validador.fgv",
    "ANALISTA":  "analista.fgv",
}


def _load_credentials() -> dict[str, tuple[str, str]]:
    """Monta o mapa role -> (user, password) a partir do ambiente.

    Perfis sem APP_<ROLE>_PASSWORD definida são omitidos (login indisponível),
    em vez de cair num default embutido no código.
    """
    creds: dict[str, tuple[str, str]] = {}
    for role in ROLES:
        pwd = os.environ.get(f"APP_{role}_PASSWORD")
        if not pwd:
            continue
        user = os.environ.get(f"APP_{role}_USER", _DEFAULT_USERS[role])
        creds[role] = (user, pwd)
    return creds


CREDENTIALS = _load_credentials()


def verify_login(nome: str, senha: str) -> dict | None:
    for role, (user, pwd) in CREDENTIALS.items():
        if nome == user and senha == pwd:
            return {"nome": user, "role": role}
    return None
