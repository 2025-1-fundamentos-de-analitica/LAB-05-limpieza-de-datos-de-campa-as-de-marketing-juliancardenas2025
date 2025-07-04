"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel


import os
import zipfile
import pandas as pd

def clean_campaign_data():
    # Crear la carpeta output si no existe
    os.makedirs("files/output", exist_ok=True)

    # Lista para almacenar los DataFrames de cada archivo CSV
    dfs = []

    # Procesar cada archivo comprimido en la carpeta input
    for file in os.listdir("files/input"):
        if file.endswith(".zip"):
            with zipfile.ZipFile(os.path.join("files/input", file), "r") as zip_ref:
                # Leer cada archivo CSV dentro del ZIP
                for csv_file in zip_ref.namelist():
                    with zip_ref.open(csv_file) as f:
                        df = pd.read_csv(f)
                        dfs.append(df)

    # Concatenar todos los DataFrames en uno solo
    data = pd.concat(dfs, ignore_index=True)

    # Limpiar y transformar los datos para client.csv
    client_df = data[["client_id", "age", "job", "marital", "education", "credit_default", "mortgage"]].copy()
    client_df["job"] = client_df["job"].str.replace(".", "").str.replace("-", "_")
    client_df["education"] = client_df["education"].str.replace(".", "_").replace("unknown", pd.NA)
    client_df["credit_default"] = client_df["credit_default"].apply(lambda x: 1 if x == "yes" else 0)
    client_df["mortgage"] = client_df["mortgage"].apply(lambda x: 1 if x == "yes" else 0)

    # Guardar client.csv
    client_df.to_csv("files/output/client.csv", index=False)

    # Limpiar y transformar los datos para campaign.csv
    campaign_df = data[["client_id", "number_contacts", "contact_duration", "previous_campaign_contacts", "previous_outcome", "campaign_outcome", "day", "month"]].copy()
    campaign_df["previous_outcome"] = campaign_df["previous_outcome"].apply(lambda x: 1 if x == "success" else 0)
    campaign_df["campaign_outcome"] = campaign_df["campaign_outcome"].apply(lambda x: 1 if x == "yes" else 0)
    campaign_df["last_contact_date"] = pd.to_datetime(
        "2022-" + campaign_df["month"].astype(str) + "-" + campaign_df["day"].astype(str)
    )
    campaign_df = campaign_df.drop(columns=["day", "month"])

    # Guardar campaign.csv
    campaign_df.to_csv("files/output/campaign.csv", index=False)

    # Limpiar y transformar los datos para economics.csv
    economics_df = data[["client_id", "cons_price_idx", "euribor_three_months"]].copy()

    # Guardar economics.csv
    economics_df.to_csv("files/output/economics.csv", index=False)