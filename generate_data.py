"""
Script para geração de dados fictícios de coleta de produtos.
Cria o banco SQLite com duas tabelas:
  - coleta_produtos: dados brutos do scraping
  - produtos_referencia: mapeamento id_produto -> cod_bp
  - coleta_fts: índice FTS5 para busca textual eficiente
"""

import sqlite3
import random
from datetime import datetime, timedelta
from pathlib import Path

PLATAFORMAS = [
    "Mercado Livre", "Amazon", "Shopee",
    "Americanas", "Magazine Luiza", "Casas Bahia",
]

INFORMANTES = [
    ("INF001", "Supermercado Silva"),
    ("INF002", "Distribuidora Norte"),
    ("INF003", "Mercado Total"),
    ("INF004", "Atacadão Central"),
    ("INF005", "Rede Compras"),
    ("INF006", "Grupo Varejo SA"),
]

MARCAS = [
    "Nestlé", "Unilever", "P&G", "Ambev", "JBS", "BRF",
    "Natura", "L'Oréal", "Samsung", "LG", "Heinz", "Quaker",
]

CATEGORIAS = [
    "Alimentos", "Bebidas", "Higiene", "Limpeza",
    "Eletrônicos", "Vestuário", "Pet", "Farmácia",
]

DESCRICOES_BASE = [
    "Sabão em Pó", "Detergente Líquido", "Shampoo", "Condicionador",
    "Café Torrado e Moído", "Açúcar Cristal", "Farinha de Trigo",
    "Óleo de Soja", "Leite Integral", "Arroz Branco",
    "Feijão Carioca", "Macarrão Espaguete", "Molho de Tomate",
    "Refrigerante Cola", "Cerveja Lager", "Água Mineral",
    "Suco de Laranja", "Biscoito Recheado", "Chocolate ao Leite",
    "Margarina", "Manteiga", "Queijo Mussarela", "Iogurte Natural",
    "Presunto Fatiado", "Frango Congelado", "Carne Moída Bovina",
    "Linguiça Calabresa", "Pão de Forma", "Granola",
    "Cereal Matinal", "Amaciante Roupas", "Desinfetante",
]

TAMANHOS = [
    "200ml", "500ml", "1L", "2L", "1kg", "500g",
    "250g", "5kg", "3kg", "100g", "1,5L", "400g",
]

DOMINIOS = {
    "Mercado Livre": "mercadolivre.com.br",
    "Amazon": "amazon.com.br",
    "Shopee": "shopee.com.br",
    "Americanas": "americanas.com.br",
    "Magazine Luiza": "magazineluiza.com.br",
    "Casas Bahia": "casasbahia.com.br",
}


def gerar_ean() -> str:
    return "".join(str(random.randint(0, 9)) for _ in range(13))


def gerar_sku(plataforma: str, idx: int) -> str:
    prefixo = "".join(c for c in plataforma if c.isalpha())[:3].upper()
    return f"{prefixo}-{random.randint(10000, 99999)}-{idx:05d}"


def gerar_url(plataforma: str, sku: str) -> str:
    dominio = DOMINIOS.get(plataforma, "loja.com.br")
    slug = sku.lower().replace("-", "/")
    return f"https://www.{dominio}/produto/{slug}"


def gerar_preco() -> tuple:
    preco = round(random.uniform(2.99, 299.99), 2)
    preco_promo = (
        round(preco * random.uniform(0.7, 0.95), 2)
        if random.random() < 0.30
        else None
    )
    return preco, preco_promo


def criar_schema(cur: sqlite3.Cursor) -> None:
    cur.executescript("""
        CREATE TABLE IF NOT EXISTS coleta_produtos (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            data_coleta       TEXT NOT NULL,
            plataforma        TEXT,
            cod_informante    TEXT,
            nome_informante   TEXT,
            categoria         TEXT,
            ean               TEXT,
            sku               TEXT,
            url               TEXT,
            descricao         TEXT,
            marca             TEXT,
            preco             REAL,
            preco_promocional REAL,
            id_produto        TEXT,
            id_coleta         TEXT,
            id_imagem         TEXT
        );

        CREATE TABLE IF NOT EXISTS produtos_referencia (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            id_produto  TEXT,
            cod_bp      TEXT,
            descricao   TEXT,
            marca       TEXT
        );

        CREATE VIRTUAL TABLE IF NOT EXISTS coleta_fts
            USING fts5(
                id_produto UNINDEXED,
                descricao,
                marca,
                content=coleta_produtos,
                content_rowid=id
            );
    """)


def popular_coleta(cur: sqlite3.Cursor, n: int = 500) -> list:
    """Insere n produtos na tabela coleta_produtos."""
    data_inicio = datetime(2024, 1, 1)
    linhas = []

    for i in range(1, n + 1):
        plataforma = random.choice(PLATAFORMAS)
        cod_inf, nome_inf = random.choice(INFORMANTES)
        categoria = random.choice(CATEGORIAS)
        marca = random.choice(MARCAS)
        desc_base = random.choice(DESCRICOES_BASE)
        tamanho = random.choice(TAMANHOS)
        descricao = f"{desc_base} {marca} {tamanho}"
        ean = gerar_ean()
        sku = gerar_sku(plataforma, i)
        url = gerar_url(plataforma, sku)
        preco, preco_promo = gerar_preco()
        data_coleta = (
            data_inicio + timedelta(days=random.randint(0, 364))
        ).strftime("%Y-%m-%d")
        id_produto = f"PROD{i:06d}"
        id_coleta = f"COL{random.randint(1000, 9999)}"
        id_imagem = f"IMG{random.randint(10000, 99999)}"

        linhas.append((
            data_coleta, plataforma, cod_inf, nome_inf, categoria,
            ean, sku, url, descricao, marca, preco, preco_promo,
            id_produto, id_coleta, id_imagem,
        ))

    cur.executemany(
        """
        INSERT INTO coleta_produtos
            (data_coleta, plataforma, cod_informante, nome_informante, categoria,
             ean, sku, url, descricao, marca, preco, preco_promocional,
             id_produto, id_coleta, id_imagem)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        linhas,
    )
    print(f"  coleta_produtos  : {n} registros inseridos")
    return [row[12] for row in linhas]  # lista de id_produto


def popular_referencia(cur: sqlite3.Cursor) -> None:
    """Insere mapeamentos id_produto -> cod_bp (1-3 por produto)."""
    cur.execute(
        "SELECT DISTINCT id_produto, descricao, marca FROM coleta_produtos"
    )
    prods = cur.fetchall()

    refs = []
    for id_prod, desc, marca in prods:
        n_bp = random.randint(1, 3)
        for _ in range(n_bp):
            cod_bp = f"BP{random.randint(100000, 999999)}"
            refs.append((id_prod, cod_bp, desc, marca))

    cur.executemany(
        "INSERT INTO produtos_referencia (id_produto, cod_bp, descricao, marca) VALUES (?,?,?,?)",
        refs,
    )
    print(f"  produtos_referencia: {len(refs)} registros inseridos")


def popular_fts(cur: sqlite3.Cursor) -> None:
    cur.execute(
        "INSERT INTO coleta_fts(rowid, id_produto, descricao, marca) "
        "SELECT id, id_produto, descricao, marca FROM coleta_produtos"
    )
    print("  coleta_fts       : índice FTS5 criado")


def main():
    db_path = Path(__file__).parent / "data" / "database.db"
    db_path.parent.mkdir(exist_ok=True)

    if db_path.exists():
        db_path.unlink()
        print("Banco anterior removido.")

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    print("Criando schema...")
    criar_schema(cur)

    print("Inserindo dados...")
    popular_coleta(cur, n=500)
    popular_referencia(cur)
    popular_fts(cur)

    conn.commit()
    conn.close()
    print(f"\nBanco criado em: {db_path}")


if __name__ == "__main__":
    main()
