import polars as pl

# Leer archivo con separador ";"

"""
df_SMW = pl.read_csv(
    "SMW.txt",
    separator=";",
    truncate_ragged_lines=True,
    encoding="latin1",
    schema_overrides={
        "PORT_ODF_SMW": pl.Utf8
    }
)
"""

df_EAI = pl.read_csv(
    "EAI.txt",
    separator="|",
    truncate_ragged_lines=True,
    encoding="latin1",
)

df_EAI_filtrado = df_EAI.select([
    "RFS_SMW",
    "SCG_EAI",
    "NAME_OLT",
    "PORT_OLT",
    "ONTID",
    "TRAIL_PON"
])


df_RFS = pl.read_csv(
    "RFS.txt",
    separator=";",
    truncate_ragged_lines=True,
    encoding="latin1",
    schema_overrides={
        "ID_DIRECCION": pl.Utf8  # Forzar esta columna como texto
    }
)

# Filtrar columnas relevantes
df_RFS_filtrado = df_RFS.select([
    "OIT",
    "DIRECCION",
    "ID_DIRECCION",
    "NAP",
    "PORT_NAP",
    "SPLITTER_NAP",
    "PORT_SPLITTER"
])


# Mostrar las primeras filas
#print(df_RFS_filtrado.head(20))

#validar que tengan el mismo formato las columnas a comparar

df_EAI_filtrado = df_EAI_filtrado.with_columns([
    pl.col("RFS_SMW").cast(pl.Utf8)
])

df_RFS_filtrado = df_RFS_filtrado.with_columns([
    pl.col("OIT").cast(pl.Utf8)
])


valores_comunes = df_EAI_filtrado.select("RFS_SMW").unique().join(
    df_RFS_filtrado.select("OIT").unique(),
    left_on="RFS_SMW",
    right_on="OIT",
    how="inner"
)

print(f"Cantidad de claves comunes: {valores_comunes.height}")

df_data_inventario = df_EAI_filtrado.join(
    df_RFS_filtrado,
    left_on="RFS_SMW",
    right_on="OIT",
    how="inner"
).select([
    "RFS_SMW",
    "SCG_EAI",
    "NAME_OLT",
    "PORT_OLT",
    "ONTID",
    "TRAIL_PON",
    "DIRECCION",
    "ID_DIRECCION",
    "NAP",
    "PORT_NAP",
    "SPLITTER_NAP",
    "PORT_SPLITTER"
])

df_data_inventario.write_csv("resultado_unido.csv")