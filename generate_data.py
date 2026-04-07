"""
Script para geração de dados fictícios de coleta de produtos.
Cria o banco SQLite com as tabelas:
  - coleta_produtos        : dados brutos do scraping
  - coleta_cadastrado_BP   : cadastro de cod_insumo e insumo_informado por informante/produto
  - produtos_referencia    : mapeamento id_produto -> cod_bp
  - coleta_fts             : índice FTS5 para busca textual eficiente
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

TIPOS_PRECO    = ["Varejo", "Atacado", "Promoção", "Lista", "Negociado"]
PERIODICIDADES = ["Diária", "Semanal", "Quinzenal", "Mensal"]
MOEDAS         = ["BRL", "USD", "EUR"]
UFS            = [
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA",
    "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN",
    "RS", "RO", "RR", "SC", "SP", "SE", "TO",
]

TAMANHOS = [
    "200ml", "500ml", "1L", "2L", "1kg", "500g",
    "250g", "5kg", "3kg", "100g", "1,5L", "400g",
]

DOMINIOS = {
    "Mercado Livre": "mercadolivre.com.br",
    "Amazon":        "amazon.com.br",
    "Shopee":        "shopee.com.br",
    "Americanas":    "americanas.com.br",
    "Magazine Luiza":"magazineluiza.com.br",
    "Casas Bahia":   "casasbahia.com.br",
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
            periodicidade     TEXT,
            tipo_preco        TEXT,
            ean               TEXT,
            sku               TEXT,
            url               TEXT,
            descricao         TEXT,
            marca             TEXT,
            uf                TEXT,
            moeda             TEXT,
            preco             REAL,
            preco_promocional REAL,
            id_produto        TEXT,
            id_coleta         TEXT,
            id_imagem         TEXT
        );

        CREATE TABLE IF NOT EXISTS coleta_cadastrado_BP (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            cod_informante    TEXT NOT NULL,
            id_produto        TEXT NOT NULL,
            cod_insumo        TEXT,
            insumo_informado  INTEGER
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
    data_inicio = datetime(2024, 5, 1)
    linhas = []

    for i in range(1, n + 1):
        plataforma   = random.choice(PLATAFORMAS)
        cod_inf, nome_inf = random.choice(INFORMANTES)
        marca        = random.choice(MARCAS)
        desc_base    = random.choice(DESCRICOES_BASE)
        tamanho      = random.choice(TAMANHOS)
        descricao    = f"{desc_base} {marca} {tamanho}"
        ean          = gerar_ean()
        sku          = gerar_sku(plataforma, i)
        url          = gerar_url(plataforma, sku)
        preco, preco_promo = gerar_preco()
        periodicidade = random.choice(PERIODICIDADES)
        tipo_preco   = random.choice(TIPOS_PRECO)
        uf           = random.choice(UFS)
        moeda        = random.choice(MOEDAS)
        data_coleta  = (
            data_inicio + timedelta(days=random.randint(0, 730))
        ).strftime("%Y-%m-%d")
        id_produto   = f"PROD{i:06d}"
        id_coleta    = f"COL{random.randint(1000, 9999)}"
        id_imagem    = f"IMG{random.randint(10000, 99999)}"

        linhas.append((
            data_coleta, plataforma, cod_inf, nome_inf, periodicidade, tipo_preco,
            ean, sku, url, descricao, marca, uf, moeda,
            preco, preco_promo, id_produto, id_coleta, id_imagem,
        ))

    cur.executemany(
        """
        INSERT INTO coleta_produtos
            (data_coleta, plataforma, cod_informante, nome_informante, periodicidade, tipo_preco,
             ean, sku, url, descricao, marca, uf, moeda,
             preco, preco_promocional, id_produto, id_coleta, id_imagem)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        linhas,
    )
    print(f"  coleta_produtos     : {n} registros inseridos")
    # Retorna lista de (cod_informante, id_produto)
    return [(row[2], row[15]) for row in linhas]


def popular_cadastrado_bp(cur: sqlite3.Cursor, pares: list) -> None:
    """
    Popula coleta_cadastrado_BP com três cenários:
      - 40% : cod_insumo + insumo_informado preenchidos
      - 30% : apenas cod_insumo preenchido
      - 30% : nenhum dos dois (linha existe mas campos nulos)
    """
    random.shuffle(pares)
    n = len(pares)
    corte1 = int(n * 0.40)
    corte2 = int(n * 0.70)

    registros = []
    for idx, (cod_inf, id_prod) in enumerate(pares):
        if idx < corte1:
            # ambos preenchidos
            cod_insumo       = f"INS{random.randint(10000, 99999)}"
            insumo_informado = random.randint(100000, 999999)
        elif idx < corte2:
            # só cod_insumo
            cod_insumo       = f"INS{random.randint(10000, 99999)}"
            insumo_informado = None
        else:
            # nenhum
            cod_insumo       = None
            insumo_informado = None

        registros.append((cod_inf, id_prod, cod_insumo, insumo_informado))

    cur.executemany(
        """
        INSERT INTO coleta_cadastrado_BP
            (cod_informante, id_produto, cod_insumo, insumo_informado)
        VALUES (?,?,?,?)
        """,
        registros,
    )
    c_ambos = sum(1 for r in registros if r[2] and r[3])
    c_so    = sum(1 for r in registros if r[2] and not r[3])
    c_none  = sum(1 for r in registros if not r[2])
    print(f"  coleta_cadastrado_BP: {len(registros)} registros "
          f"(ambos={c_ambos}, só cod_insumo={c_so}, sem código={c_none})")


def popular_referencia(cur: sqlite3.Cursor) -> None:
    """Insere mapeamentos id_produto -> cod_bp (1-3 por produto)."""
    cur.execute(
        "SELECT DISTINCT id_produto, descricao, marca FROM coleta_produtos"
    )
    prods = cur.fetchall()

    refs = []
    for id_prod, desc, marca in prods:
        for _ in range(random.randint(1, 3)):
            cod_bp = f"BP{random.randint(100000, 999999)}"
            refs.append((id_prod, cod_bp, desc, marca))

    cur.executemany(
        "INSERT INTO produtos_referencia (id_produto, cod_bp, descricao, marca) VALUES (?,?,?,?)",
        refs,
    )
    print(f"  produtos_referencia : {len(refs)} registros inseridos")


def popular_fts(cur: sqlite3.Cursor) -> None:
    cur.execute(
        "INSERT INTO coleta_fts(rowid, id_produto, descricao, marca) "
        "SELECT id, id_produto, descricao, marca FROM coleta_produtos"
    )
    print("  coleta_fts          : índice FTS5 criado")


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
    pares = popular_coleta(cur, n=500)
    popular_cadastrado_bp(cur, pares)
    popular_referencia(cur)
    popular_fts(cur)

    conn.commit()
    conn.close()
    print(f"\nBanco criado em: {db_path}")


if __name__ == "__main__":
    main()
