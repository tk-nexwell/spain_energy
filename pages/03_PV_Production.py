from datetime import datetime
import sqlite3

import altair as alt
import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="PV Production",
    page_icon=":sun_with_face:",
    layout="wide"
)

# Display logo in sidebar
try:
    st.sidebar.image("NP_logo.svg", use_container_width=True)
except Exception:
    pass  # Logo file not found, continue without it

# Apply brand styling
from style_config import apply_brand_styling
apply_brand_styling()

from captured_prices import PV_DB, list_pv_profiles
from chart_config import BRAND_COLOR

# Different shades of brand green for PV profiles
PV_COLORS = {
    "PV 1.2 DC/AC": "#439644",  # Original brand color (pv1)
    "PV 1.5 DC/AC": "#5CB85C",  # Lighter green (pv2)
    "PV 2.0 DC/AC": "#2E7D32",  # Darker green (pv3)
}


def _profile_label(name: str) -> str:
    # Note: pv2 and pv3 columns have been swapped in the database
    mapping = {
        "pv1": "PV 1.2 DC/AC",
        "pv2": "PV 2.0 DC/AC",  # pv2 now contains what was pv3
        "pv3": "PV 1.5 DC/AC",  # pv3 now contains what was pv2
    }
    return mapping.get(name, name)


def load_pv_profiles_long() -> pd.DataFrame:
    """Load pv_profiles table as a long DataFrame: month, day, hour, profile, pv_mwh."""
    conn = sqlite3.connect(PV_DB)
    df = pd.read_sql("SELECT * FROM pv_profiles", conn)
    conn.close()

    profiles = [c for c in df.columns if c not in ("month", "day", "hour")]
    if not profiles:
        return pd.DataFrame(columns=["month", "day", "hour", "profile", "pv_mwh"])

    long_rows = []
    for p in profiles:
        tmp = df[["month", "day", "hour", p]].copy()
        tmp = tmp.rename(columns={p: "pv_mwh"})
        tmp["profile"] = p
        long_rows.append(tmp)

    out = pd.concat(long_rows, ignore_index=True)
    out.dropna(subset=["pv_mwh"], inplace=True)
    out["month"] = out["month"].astype(int)
    out["day"] = out["day"].astype(int)
    out["hour"] = out["hour"].astype(int)
    return out


def main() -> None:
    st.title("PV Production")
    
    st.markdown("""
    This page displays PV production profiles showing the typical output patterns across different time periods.
    PV output varies by season (month) and time of day, reflecting solar irradiance patterns.
    """)

    pv_profiles = list_pv_profiles()
    if not pv_profiles:
        st.warning("No PV profiles found in the database.")
        return

    st.sidebar.header("PV Profile")
    selected_profile = st.sidebar.selectbox(
        "Profile for detailed view",
        options=pv_profiles,
        format_func=_profile_label,
        index=0,
    )

    df_long = load_pv_profiles_long()
    if df_long.empty:
        st.info("No PV production data available.")
        return

    # Total yearly production per profile (MWh)
    st.subheader("Total yearly PV production per profile")
    yearly = df_long.groupby("profile", as_index=False)["pv_mwh"].sum()
    yearly["pv_mwh"] = yearly["pv_mwh"] / 1000.0  # convert to MWh
    yearly["profile_label"] = yearly["profile"].apply(_profile_label)
    chart_yearly = (
        alt.Chart(yearly)
        .mark_bar()
        .encode(
            x=alt.X("profile_label:N", title="Profile"),
            y=alt.Y("pv_mwh:Q", title="MWh/year"),
            color=alt.Color(
                "profile_label:N",
                scale=alt.Scale(
                    domain=list(PV_COLORS.keys()),
                    range=list(PV_COLORS.values())
                ),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("profile_label:N", title="Profile"),
                alt.Tooltip("pv_mwh:Q", title="Total output (MWh)", format=".1f"),
            ],
        )
        .properties(height=250)
    )
    st.altair_chart(chart_yearly, use_container_width=True)

    # Seasonality across months
    st.subheader("Seasonality by month")
    monthly = df_long.copy()
    monthly["month_name"] = monthly["month"].apply(
        lambda m: datetime(2000, m, 1).strftime("%b")
    )
    monthly_agg = (
        monthly.groupby(["month", "month_name", "profile"], as_index=False)["pv_mwh"]
        .sum()
    )
    monthly_agg["pv_mwh"] = monthly_agg["pv_mwh"] / 1000.0  # to MWh/month
    monthly_agg["profile_label"] = monthly_agg["profile"].apply(_profile_label)

    chart_month = (
        alt.Chart(monthly_agg)
        .mark_line()
        .encode(
            x=alt.X(
                "month:O",
                title="Month",
                axis=alt.Axis(
                    values=list(range(1, 13)),
                    labelExpr="datum.value",
                    labelAngle=0,
                ),
            ),
            y=alt.Y("pv_mwh:Q", title="MWh/month"),
            color=alt.Color(
                "profile_label:N",
                title="Profile",
                scale=alt.Scale(
                    domain=list(PV_COLORS.keys()),
                    range=list(PV_COLORS.values())
                )
            ),
            tooltip=[
                alt.Tooltip("profile_label:N", title="Profile"),
                alt.Tooltip("month_name:N", title="Month"),
                alt.Tooltip("pv_mwh:Q", title="Total output (MWh)", format=".1f"),
            ],
        )
        .properties(height=250)
    )
    st.altair_chart(chart_month, use_container_width=True)

    # Seasonality across time of day: typical day per profile
    st.subheader("Typical day profiles by hour of day")
    hod = (
        df_long.groupby(["hour", "profile"], as_index=False)["pv_mwh"]
        .mean()
        .copy()
    )
    hod["profile_label"] = hod["profile"].apply(_profile_label)
    chart_hod = (
        alt.Chart(hod)
        .mark_line()
        .encode(
            x=alt.X("hour:O", title="Hour of day"),
            y=alt.Y("pv_mwh:Q", title="MWh/hour"),
            color=alt.Color(
                "profile_label:N",
                title="Profile",
                scale=alt.Scale(
                    domain=list(PV_COLORS.keys()),
                    range=list(PV_COLORS.values())
                )
            ),
            tooltip=[
                alt.Tooltip("profile_label:N", title="Profile"),
                alt.Tooltip("hour:O", title="Hour"),
                alt.Tooltip("pv_mwh:Q", title="Average output (kWh)", format=".2f"),
            ],
        )
        .properties(height=250)
    )
    st.altair_chart(chart_hod, use_container_width=True)

    # Heatmap for selected profile: month vs hour
    st.subheader(f"Seasonality heatmap for {_profile_label(selected_profile)}")
    df_sel = df_long[df_long["profile"] == selected_profile].copy()
    heat = (
        df_sel.groupby(["month", "hour"], as_index=False)["pv_mwh"]
        .mean()
        .copy()
    )
    heat["pv_mwh"] = heat["pv_mwh"] / 1000.0  # to MWh/hour for heatmap
    heat["month_name"] = heat["month"].apply(
        lambda m: datetime(2000, m, 1).strftime("%b")
    )
    heatmap = (
        alt.Chart(heat)
        .mark_rect()
        .encode(
            x=alt.X(
                "hour:O",
                title="Hour of day",
            ),
            y=alt.Y(
                "month_name:O",
                title="Month",
                sort=[
                    "Jan",
                    "Feb",
                    "Mar",
                    "Apr",
                    "May",
                    "Jun",
                    "Jul",
                    "Aug",
                    "Sep",
                    "Oct",
                    "Nov",
                    "Dec",
                ],
            ),
            color=alt.Color(
                "pv_mwh:Q",
                title="MWh/hour",
                scale=alt.Scale(scheme="reds"),
            ),
            tooltip=[
                alt.Tooltip("month_name:O", title="Month"),
                alt.Tooltip("hour:O", title="Hour"),
                alt.Tooltip("pv_mwh:Q", title="Average output (MWh)", format=".3f"),
            ],
        )
        .properties(height=300)
    )
    st.altair_chart(heatmap, use_container_width=True)

    # Raw data and download
    st.subheader("Raw PV production data")
    st.dataframe(df_long, use_container_width=True, height=400)
    csv = df_long.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Download PV production data as CSV",
        data=csv,
        file_name="pv_production_raw.csv",
        mime="text/csv",
    )


# Streamlit automatically calls this when the page is loaded
main()


