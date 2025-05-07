
import streamlit as st
import requests
import pandas as pd
from datetime import datetime, date, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# CONFIGURAÇÕES
# ---------------------------------------------------------------------------
LOCALIDADES = [
    ( 1, "UTE Araras",                     -3.4163623, -61.3646957),
    ( 2, "UTE Augusto Montenegro",         -2.7655977, -57.7601278),
    ( 3, "UTE Barcelos",                   -0.9539114, -62.9402967),
    ( 4, "UTE Barreirinha",                -2.7899753, -57.0624469),
    ( 5, "UTE Belo Monte",                 -6.2090826, -64.2517458),
    ( 6, "UTE Beruri",                     -3.8939537, -61.3726259),
    ( 7, "UTE Boa Vista do Ramos",         -2.9748431, -57.5897374),
    ( 8, "UTE Boca do Acre",               -8.7704239, -67.3290354),
    ( 9, "UTE Cabori",                     -2.4565655, -57.1073921),
    (10, "UTE Campinas",                   -3.3121388, -61.1150544),
    (11, "UTE Canutama",                   -6.5240641, -64.3846460),
    (12, "UTE Careiro da Várzea",          -3.1972090, -59.8291053),
    (13, "UTE Carvoeiro",                  -1.3951912, -61.9790583),
    (14, "UTE Castanho I KM27",            -3.4121398, -59.9084053),
    (15, "UTE Castanho II KM100",          -3.8161042, -60.3672068),
    (16, "UTE Caviana",                    -3.7609727, -61.1547987),
    (17, "UTE Cucuí",                       1.1898817, -66.8384697),
    (18, "UTE Iauaretê",                    0.6085662, -69.1957288),
    (19, "UTE Itapuru",                   -4.2898134, -61.8048518),
    (20, "UTE Lábrea",                    -7.2802266, -64.7774113),
    (21, "UTE Lindóia",                   -2.9141720, -59.0426590),
    (22, "UTE Manaquiri",                 -3.46981883,-60.45899963),
    (23, "UTE Maués",                     -3.3775243, -57.7275584),
    (24, "UTE Mocambo",                   -2.4433143, -57.2896043),
    (25, "UTE Moura",                     -1.4564398, -61.6343814),
    (26, "UTE Nhamundá",                  -2.1988535, -56.7164244),
    (27, "UTE Novo Airão",                -2.6576009, -60.9445622),
    (28, "UTE Novo Céu",                  -3.3991027, -59.2705962),
    (29, "UTE Novo Remanso",              -3.1151898, -59.0190441),
    (30, "UTE Parauá",                    -3.1874595, -59.5011962),
    (31, "UTE Pauini",                    -7.7157490, -66.9937292),
    (32, "UTE Pedras",                    -2.7943109, -57.2689186),
    (33, "UTE Sacambú",                   -3.2758935, -60.9341430),
    (34, "UTE Santa Isabel do Rio Negro", -0.4038472, -65.0252311),
    (35, "UTE Santana do Uatumã",         -2.5776104, -57.9861587),
    (36, "UTE São Sebastião",             -2.5747721, -57.8637611),
    (37, "UTE Tapauá",                    -5.6420193, -63.1901931),
    (38, "UTE Tuiué",                     -3.6983179, -61.0736114),
    (39, "UTE Urucará",                   -2.5396935, -57.7450827),
    (40, "UTE Urucurituba",               -3.1342579, -58.1598098),
    (41, "UTE Vila Amazônia",             -2.6155236, -56.6707780),
    (42, "UTE Vila Urucurituba",          -3.5476129, -58.9296339),
]
ARCHIVE_DELAY = 7      # dias para usar endpoint /archive
MAX_FORECAST = 16      # dias no futuro permitidos
HOURLY_VARS = "temperature_2m,precipitation,precipitation_probability"

# Sessão HTTP com retry
_retry = Retry(total=5, backoff_factor=1.5,
               status_forcelist=[502, 503, 504],
               allowed_methods=["GET"])
session = requests.Session()
session.mount("https://", HTTPAdapter(max_retries=_retry))

def _get_df(url: str) -> pd.DataFrame:
    r = session.get(url, timeout=(10, 120))
    r.raise_for_status()
    j = r.json()
    return pd.DataFrame({
        "datetime": j["hourly"]["time"],
        "temperature_C": j["hourly"]["temperature_2m"],
        "precip_mm": j["hourly"]["precipitation"],
        "precip_prob_%": j["hourly"]["precipitation_probability"],
    })


def baixa_dados(lat, lon, data_inicio, dias, tz="auto", arquivo="tempo.xlsx"):
    dt_ini = datetime.strptime(data_inicio, "%Y-%m-%d").date()
    dt_fim = dt_ini + timedelta(days=dias - 1)
    hoje = date.today()
    limite_hist = hoje - timedelta(days=ARCHIVE_DELAY)

    dfs = []
    # Histórico
    if dt_ini <= limite_hist:
        hist_end = min(dt_fim, limite_hist)
        url_hist = (
            f"https://archive-api.open-meteo.com/v1/archive"
            f"?latitude={lat}&longitude={lon}"
            f"&start_date={dt_ini}&end_date={hist_end}"
            f"&hourly={HOURLY_VARS}&timezone={tz}"
        )
        dfs.append(_get_df(url_hist))

    # Previsão + últimos 14 dias
    if dt_fim > limite_hist:
        inicio_prev = max(dt_ini, limite_hist + timedelta(days=1))
        past_days = max((hoje - inicio_prev).days, 0)
        forecast_d = max((dt_fim - hoje).days + 1, 0)
        if forecast_d > MAX_FORECAST:
            raise ValueError("Período futuro excede 16 dias.")
        if past_days > 14:
            raise ValueError("Parte passada > 14 dias.")
        url_prev = (
            f"https://api.open-meteo.com/v1/forecast"
            f"?latitude={lat}&longitude={lon}"
            f"&hourly={HOURLY_VARS}&timezone={tz}"
            + (f"&past_days={past_days}" if past_days else "")
            + (f"&forecast_days={forecast_d}" if forecast_d else "")
        )
        df_prev = _get_df(url_prev)
        mask = (
            (df_prev["datetime"] >= f"{inicio_prev}T00:00") &
            (df_prev["datetime"] <= f"{dt_fim}T23:59")
        )
        dfs.append(df_prev.loc[mask])

    if not dfs:
        raise ValueError("Período fora do alcance da API.")

    df_final = pd.concat(dfs).sort_values("datetime").reset_index(drop=True)
    df_final["datetime"] = pd.to_datetime(df_final["datetime"]).dt.tz_localize(None)

    # Salvar Excel
    with pd.ExcelWriter(
        arquivo, engine="xlsxwriter",
        datetime_format="dd/mm/yyyy hh:mm:ss",
        date_format="dd/mm/yyyy"
    ) as writer:
        df_final.to_excel(writer, index=False)

    return df_final

# ---------------------------------------------------------------------------
# INTERFACE STREAMLIT
# ---------------------------------------------------------------------------
st.title("Consulta de Previsão de Tempo - Open Meteo")

loc_options = {i: (nome, lat, lon) for i, nome, lat, lon in LOCALIDADES}
selected = st.selectbox("Localidade:", options=list(loc_options.keys()), format_func=lambda x: loc_options[x][0])

# Inputs
data_inicio = st.date_input("Data Início (YYYY-MM-DD)", value=date.today() - timedelta(days=1))
dias = st.number_input("Quantidade de Dias", min_value=1, value=1)

# Ação ao clicar
if st.button("Baixar e Salvar Excel"):
    tag_data = data_inicio.strftime("%Y%m%d")
    nome_loc = loc_options[selected][0].replace(' ', '_')
    arquivo = f"{nome_loc}_{tag_data}.xlsx"
    with st.spinner("Baixando dados..."):
        df = baixa_dados(loc_options[selected][1], loc_options[selected][2], data_inicio.strftime("%Y-%m-%d"), dias, arquivo=arquivo)
    st.success(f"Arquivo salvo: {arquivo}")
    # Filtrar precip_prob > 75%
    high_df = df[df['precip_prob_%'] > 75].sort_values('datetime', ascending=False)
    if not high_df.empty:
        st.subheader("Horários com probabilidade de precipitação > 75%")
        st.dataframe(high_df[['datetime', 'precip_prob_%']])
    else:
        st.info("Nenhum horário com probabilidade de precipitação acima de 75%.")

    # Botão de download
    with open(arquivo, 'rb') as f:
        st.download_button(
            label='Download do Excel',
            data=f,
            file_name=arquivo,
            mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
