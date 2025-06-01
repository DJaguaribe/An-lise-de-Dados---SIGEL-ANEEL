try:
    import requests
    import pandas as pd
    import geopandas as gpd
    from shapely.geometry import Point
    from unidecode import unidecode
    import os
    import re
    from typing import Dict, Any, Optional
except ImportError:
    import subprocess
    import sys
    print("[INFO] Instalando dependências...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "requests", "pandas", "geopandas", "shapely", "unidecode"])
    import requests
    import pandas as pd
    import geopandas as gpd
    from shapely.geometry import Point
    from unidecode import unidecode
    import os
    import re
    from typing import Dict, Any, Optional

def build_query_params() -> Dict[str, str]:
    return {
        "where": "1=1",
        "outFields": "*",
        "f": "json",
        "outSR": "4326",
        "units": "esriMeters"
    }

def fetch_all_data(url: str, params: Dict[str, str], page_size: int = 1000) -> Optional[Dict[str, Any]]:
    all_features = []
    offset = 0
    print("[INFO] Iniciando download paginado dos dados...")

    while True:
        params.update({
            "resultOffset": str(offset),
            "resultRecordCount": str(page_size)
        })
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            features = data.get("features", [])
            if not features:
                break
            all_features.extend(features)
            print(f"[INFO] Página com {len(features)} registros coletada (offset={offset})")
            if len(features) < page_size:
                break
            offset += page_size
        except requests.RequestException as e:
            print(f"[ERRO] Falha na requisição: {e}")
            return None

    print(f"[INFO] Total de registros coletados: {len(all_features)}")
    return {"features": all_features}

def json_to_dataframe(data: Dict[str, Any]) -> pd.DataFrame:
    features = data.get("features", [])
    attributes = [f["attributes"] for f in features]
    geometries = [f["geometry"] for f in features]
    df = pd.DataFrame(attributes)
    df["longitude"] = [g.get("x") for g in geometries]
    df["latitude"] = [g.get("y") for g in geometries]
    return df

def dataframe_to_geodataframe(df: pd.DataFrame) -> gpd.GeoDataFrame:
    geometry = [Point(xy) for xy in zip(df["longitude"], df["latitude"])]
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
    return gdf

def tratar_dados(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    gdf = gdf.drop_duplicates()
    gdf = gdf[gdf.geometry.notnull()]
    gdf = gdf[gdf.is_valid]
    gdf.columns = [col.strip().lower() for col in gdf.columns]

    for col in gdf.select_dtypes(include='object').columns:
        gdf[col] = gdf[col].apply(lambda x: unidecode(str(x)) if isinstance(x, str) else x)


    if "operacao" in gdf.columns:
        gdf = gdf[~gdf["operacao"].isna()]             # Remove nulos
        gdf = gdf[gdf["operacao"] != "1"]              # Remove "1"


    if "data_atualizacao" in gdf.columns:
        try:
            data_convertida = pd.to_datetime(gdf["data_atualizacao"], unit='ms')
            gdf["data_atualizacao"] = data_convertida.dt.strftime('%d/%m/%Y')
            gdf["dia_atualizacao"] = data_convertida.dt.day
            gdf["mes_atualizacao"] = data_convertida.dt.month
            gdf["ano_atualizacao"] = data_convertida.dt.year
        except Exception as e:
            print(f"[AVISO] Erro ao processar data_atualizacao: {e}")

    return gdf


def validar_dados(gdf: gpd.GeoDataFrame) -> None:
    print("\n[VALIDAÇÃO DOS DADOS]")
    print(f"Total de registros: {len(gdf)}")
    print(f"Geometrias válidas: {gdf.is_valid.all()}")
    print(f"CRS (Sistema de referência espacial): {gdf.crs}\n")
    print("Tipos de dados:")
    print(gdf.dtypes)
    print("\nVisualização das 5 primeiras linhas:")
    print(gdf.head())

def save_geodata(gdf: gpd.GeoDataFrame, filename: str) -> None:
    os.makedirs("Outputs", exist_ok=True)
    caminho = os.path.join("Outputs", filename)
    gdf.to_file(caminho, driver="GeoJSON")
    print(f"[INFO] Arquivo GeoJSON salvo como: {caminho}")

def save_csv(df: pd.DataFrame, filename: str) -> None:
    os.makedirs("Outputs", exist_ok=True)
    caminho = os.path.join("Outputs", filename)
    df.to_csv(caminho, index=False)
    print(f"[INFO] Arquivo CSV salvo como: {caminho}")

def main() -> None:
    input("Olá! Você executou o algoritmo de Extração de dados dos aerogeradores do ArcGIS do SIGEL/ANEEL. Pressione qualquer tecla para iniciar o processo...")

    url = "https://sigel.aneel.gov.br/arcgis/rest/services/PORTAL/WFS/MapServer/0/query"
    params = build_query_params()

    raw_data = fetch_all_data(url, params)
    if not raw_data:
        print("[ERRO] Nenhum dado retornado.")
        return

    df = json_to_dataframe(raw_data)
    gdf = dataframe_to_geodataframe(df)

    gdf = tratar_dados(gdf)
    validar_dados(gdf)

    confirm = input("\nDeseja salvar os dados tratados? (s/N): ").strip().lower()
    if confirm != 's':
        print("[INFO] Salvamento cancelado pelo usuário.")
        return

    save_geodata(gdf, "aerogeradores_SIGEL_tratado.geojson")
    save_csv(gdf.drop(columns="geometry"), "aerogeradores_SIGEL_tratado.csv")
    print("[INFO] Processo concluído com sucesso!")

if __name__ == "__main__":
    main()
