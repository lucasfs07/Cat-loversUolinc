import argparse
from pathlib import Path
import pandas as pd
import requests

#CONSTANTES
api_base = "https://cat-fact.herokuapp.com"
routes = {"all": "/facts", "random": "/facts/random"}
species_validas = {"cat", "dog", "horse", "snail"}
arquivo_saida = Path("ETLs/catFacts.csv")

def especie_valida(especie):
    return especie.lower() in species_validas
# Funcoes principais
def buscar_aleatorios(especie="cat", quantidade=1):
    especie = especie.lower()
    if not especie_valida(especie):
        raise ValueError(f"especie '{especie}' nao suportada")

    resp = requests.get(
        f"{api_base}{routes['random']}",
        params={"animal_type": especie, "amount": quantidade},
        timeout=10
    )
    resp.raise_for_status()
    dados = resp.json()
    return dados if isinstance(dados, list) else [dados]

def buscar_todos():
    resp = requests.get(f"{api_base}{routes['all']}", timeout=15)
    resp.raise_for_status()
    return resp.json()
# salvar em csv
def salvar_csv(registros, append=False):
    if not registros:
        return

    df_novo = pd.DataFrame(registros)

    if append and arquivo_saida.exists():
        df_antigo = pd.read_csv(arquivo_saida)
        df_final = pd.concat([df_antigo, df_novo]).drop_duplicates(subset=["_id"])
    else:
        df_final = df_novo

    arquivo_saida.parent.mkdir(parents=True, exist_ok=True)
    df_final.to_csv(arquivo_saida, index=False)

def main():
    parser = argparse.ArgumentParser(description="coleta fatos de animais")
    
    parser.add_argument("-a", "--all", action="store_true", help="baixar todos os fatos")
    parser.add_argument("-r", "--random", action="store_true", help="buscar fatos aleatorios default")
    parser.add_argument("-s", "--species", default="cat", help="animal default = cat")
    parser.add_argument("-n", "--number", type=int, default=1, help="quantidade de aleatorios")
    parser.add_argument("--append", action="store_true", help="acrescentar ao csv existente")

    args = parser.parse_args()

    try:
        # se nada for passado = 1 fato aleatorio
        if args.all:
            fatos = buscar_todos()
        else:
            fatos = buscar_aleatorios(args.species, args.number)

        salvar_csv(fatos, args.append)
        print(f"pronto → {len(fatos)} fato(s) salvo(s) em {arquivo_saida.resolve()}")

    except Exception as e:
        print(f"erro: {e}")

if __name__ == "__main__":
    main()