"""
NYC Taxi - Lambda Architecture Dashboard
Streaming en temps reel (gauche) | Batch Reprocessing (droite)
Auto-refresh toutes les 3 secondes pour les donnees streaming
"""

import time
import datetime
import streamlit as st
import clickhouse_connect
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

# ── Config page ──
st.set_page_config(
    page_title="NYC Taxi - Lambda Dashboard",
    layout="wide",
    initial_sidebar_state="collapsed"
)

st.title("NYC Taxi - Architecture Lambda")

# Horloge live pour montrer que le dashboard se rafraichit
import datetime
st.caption(f"Streaming temps reel (gauche) | Batch Reprocessing (droite) | Derniere MAJ : **{datetime.datetime.now().strftime('%H:%M:%S')}**")

# ── Connexion ClickHouse (nouvelle a chaque refresh pour donnees fraiches) ──
def get_client():
    return clickhouse_connect.get_client(host='clickhouse', port=8123)

try:
    client = get_client()
except Exception as e:
    st.error(f"Connexion ClickHouse echouee : {e}")
    st.stop()

# ── Helper ──
def query_df(sql):
    try:
        result = client.query(sql)
        if result.result_rows:
            return pd.DataFrame(result.result_rows, columns=result.column_names)
        return pd.DataFrame()
    except Exception as e:
        return pd.DataFrame()


# ═══════════════════════════════════════
# ONGLETS
# ═══════════════════════════════════════
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Streaming vs Batch",
    "Analyse Historique",
    "ML Insights",
    "Alertes Live",
    "Controle Batch"
])


# ───────────────────────────────────────
# TAB 1 : Streaming (live) vs Batch Reprocessing (exact)
# ───────────────────────────────────────
with tab1:
    col_stream, col_batch = st.columns(2)

    # ── COLONNE GAUCHE : STREAMING TEMPS REEL ──
    with col_stream:
        st.subheader("Streaming (temps reel)")
        st.caption("Donnees de Decembre - mises a jour en continu")

        df_s = query_df("SELECT * FROM stream_borough_stats FINAL ORDER BY nb_trips DESC")
        if not df_s.empty:
            # Metriques live
            cols = st.columns(3)
            cols[0].metric("Trajets live", f"{df_s['nb_trips'].sum():,.0f}")
            cols[1].metric("Revenu live", f"${df_s['total_revenue'].sum():,.0f}")
            cols[2].metric("Distance moy.", f"{df_s['avg_distance'].mean():.1f} mi")

            # Graphique borough
            fig = px.bar(df_s, x='borough', y='nb_trips',
                        color='borough', title="Trajets par Borough (Live)",
                        color_discrete_sequence=px.colors.qualitative.Set1)
            fig.update_layout(showlegend=False, height=350)
            st.plotly_chart(fig, use_container_width=True)

            # Graphique revenus
            fig2 = px.bar(df_s, x='borough', y='total_revenue',
                         color='borough', title="Revenus par Borough (Live $)",
                         color_discrete_sequence=px.colors.qualitative.Set1)
            fig2.update_layout(showlegend=False, height=300)
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("En attente du streaming... Les donnees apparaitront automatiquement.")

        # Top zones streaming
        df_tz = query_df("SELECT * FROM stream_top_zones FINAL ORDER BY nb_trips DESC LIMIT 10")
        if not df_tz.empty:
            fig_tz = px.bar(df_tz, x='nb_trips', y='zone', orientation='h',
                           color='borough', title="Top 10 Zones (Live)",
                           color_discrete_sequence=px.colors.qualitative.Pastel)
            fig_tz.update_layout(height=350, yaxis=dict(autorange='reversed'))
            st.plotly_chart(fig_tz, use_container_width=True)

    # ── COLONNE DROITE : BATCH REPROCESSING ──
    with col_batch:
        st.subheader("Batch Reprocessing (exact)")
        st.caption("Recalcul complet des donnees streaming accumulees")

        df_r = query_df("SELECT * FROM reprocess_borough_stats ORDER BY nb_trips DESC")
        if not df_r.empty:
            # Metriques batch
            cols = st.columns(3)
            cols[0].metric("Trajets (batch)", f"{df_r['nb_trips'].sum():,.0f}")
            cols[1].metric("Revenu (batch)", f"${df_r['total_revenue'].sum():,.0f}")
            cols[2].metric("Distance moy.", f"{df_r['avg_distance'].mean():.1f} mi")

            # Graphique borough
            fig = px.bar(df_r, x='borough', y='nb_trips',
                        color='borough', title="Trajets par Borough (Batch Exact)",
                        color_discrete_sequence=px.colors.qualitative.Set2)
            fig.update_layout(showlegend=False, height=350)
            st.plotly_chart(fig, use_container_width=True)

            # Graphique revenus
            fig2 = px.bar(df_r, x='borough', y='total_revenue',
                         color='borough', title="Revenus par Borough (Batch Exact $)",
                         color_discrete_sequence=px.colors.qualitative.Set2)
            fig2.update_layout(showlegend=False, height=300)
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("Pas encore de batch reprocessing. Cliquez sur le bouton dans l'onglet 'Controle Batch'.")

        # Stats par jour (batch reprocess)
        df_daily = query_df("SELECT * FROM reprocess_daily_stats ORDER BY day")
        if not df_daily.empty:
            fig = px.bar(df_daily, x='day', y='nb_trips',
                        title="Trajets par Jour de Decembre (Batch Exact)",
                        color='total_revenue', color_continuous_scale='Blues')
            fig.update_layout(height=350, xaxis_title="Jour", yaxis_title="Nb Trajets")
            st.plotly_chart(fig, use_container_width=True)

    # ── Comparaison Streaming vs Batch (en bas) ──
    if not df_s.empty and not df_r.empty:
        st.markdown("---")
        st.subheader("Comparaison : Streaming vs Batch")
        st.caption("Le streaming est approximatif (incremental), le batch est exact (recalcul total)")

        merged = df_s[['borough', 'nb_trips']].merge(
            df_r[['borough', 'nb_trips']],
            on='borough', suffixes=('_stream', '_batch'), how='outer'
        ).fillna(0)

        fig_comp = go.Figure()
        fig_comp.add_trace(go.Bar(
            x=merged['borough'], y=merged['nb_trips_stream'],
            name='Streaming (live)', marker_color='#E45756'
        ))
        fig_comp.add_trace(go.Bar(
            x=merged['borough'], y=merged['nb_trips_batch'],
            name='Batch (exact)', marker_color='#72B7B2'
        ))
        fig_comp.update_layout(
            title="Trajets : Streaming vs Batch par Borough",
            barmode='group', height=350
        )
        st.plotly_chart(fig_comp, use_container_width=True)


# ───────────────────────────────────────
# TAB 2 : Analyse Historique (batch mois 1-11)
# ───────────────────────────────────────
with tab2:
    st.subheader("Historique Batch (Janvier - Novembre)")
    st.caption("Resultats du traitement batch sur 11 mois de donnees")

    col1, col2 = st.columns(2)

    with col1:
        # Stats par borough
        df = query_df("SELECT * FROM batch_stats_borough ORDER BY nb_trips DESC")
        if not df.empty:
            fig = px.bar(df, x='borough', y='nb_trips',
                        color='borough', title="Trajets par Borough (11 mois)",
                        color_discrete_sequence=px.colors.qualitative.Set2)
            fig.update_layout(showlegend=False, height=350)
            st.plotly_chart(fig, use_container_width=True)

            cols = st.columns(3)
            cols[0].metric("Total trajets", f"{df['nb_trips'].sum():,.0f}")
            cols[1].metric("Revenu total", f"${df['total_revenue'].sum():,.0f}")
            cols[2].metric("Distance moy.", f"{df['avg_distance'].mean():.1f} mi")

        # Top zones
        df_zones = query_df("SELECT * FROM batch_top_zones ORDER BY nb_trips DESC LIMIT 10")
        if not df_zones.empty:
            fig = px.bar(df_zones, x='nb_trips', y='zone', orientation='h',
                        color='borough', title="Top 10 Zones de Pickup",
                        color_discrete_sequence=px.colors.qualitative.Bold)
            fig.update_layout(height=400, yaxis=dict(autorange='reversed'))
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Stats mensuelles
        df_month = query_df("SELECT * FROM batch_stats_monthly ORDER BY year, month")
        if not df_month.empty:
            mois_noms = {1:'Jan',2:'Fev',3:'Mar',4:'Avr',5:'Mai',6:'Jun',
                        7:'Jul',8:'Aou',9:'Sep',10:'Oct',11:'Nov',12:'Dec'}
            df_month['mois'] = df_month['month'].map(mois_noms)

            fig2 = go.Figure()
            fig2.add_trace(go.Bar(x=df_month['mois'], y=df_month['nb_trips'],
                                  name='Trajets', marker_color='steelblue'))
            fig2.add_trace(go.Scatter(x=df_month['mois'], y=df_month['total_revenue'],
                                      name='Revenu ($)', yaxis='y2',
                                      line=dict(color='orange', width=3)))
            fig2.update_layout(
                title="Evolution Mensuelle (Jan-Nov)",
                yaxis=dict(title='Nombre de trajets'),
                yaxis2=dict(title='Revenu ($)', overlaying='y', side='right'),
                height=350
            )
            st.plotly_chart(fig2, use_container_width=True)

        # Matrice OD
        df_od = query_df("SELECT * FROM batch_od_matrix ORDER BY nb_trips DESC LIMIT 15")
        if not df_od.empty:
            df_od['route'] = df_od['zone_pickup'] + ' -> ' + df_od['zone_dropoff']
            fig = px.bar(df_od, x='nb_trips', y='route', orientation='h',
                        title="Top 15 Routes (Origine-Destination)",
                        color='avg_fare', color_continuous_scale='Viridis')
            fig.update_layout(height=400, yaxis=dict(autorange='reversed'))
            st.plotly_chart(fig, use_container_width=True)

    # Impact meteo + Patterns jour
    col3, col4 = st.columns(2)
    with col3:
        df_meteo = query_df("SELECT * FROM batch_stats_weather ORDER BY nb_trips DESC")
        if not df_meteo.empty:
            fig = px.scatter(df_meteo, x='temp_max', y='nb_trips',
                           size='precipitation', color='avg_fare',
                           title="Impact Meteo sur l'Activite",
                           labels={'temp_max':'Temperature Max (C)', 'nb_trips':'Nb Trajets'},
                           color_continuous_scale='RdYlGn_r')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

    with col4:
        df_day = query_df("SELECT * FROM batch_day_patterns ORDER BY day_of_week, borough")
        if not df_day.empty:
            jour_noms = {1:'Lun',2:'Mar',3:'Mer',4:'Jeu',5:'Ven',6:'Sam',7:'Dim'}
            df_day['jour'] = df_day['day_of_week'].map(jour_noms)
            fig = px.bar(df_day, x='jour', y='nb_trips', color='borough',
                        title="Activite par Jour de Semaine",
                        barmode='stack',
                        color_discrete_sequence=px.colors.qualitative.Set2)
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)


# ───────────────────────────────────────
# TAB 3 : ML Insights
# ───────────────────────────────────────
with tab3:
    col1, col2 = st.columns(2)

    with col1:
        df_cl = query_df("SELECT * FROM batch_clusters ORDER BY cluster_id")
        if not df_cl.empty:
            fig = px.scatter(df_cl, x='center_distance', y='center_fare',
                           size='nb_trips', color='label',
                           title="Clusters de Trajets (K-Means)",
                           labels={'center_distance':'Distance Moyenne (mi)',
                                   'center_fare':'Tarif Moyen ($)'},
                           size_max=60)
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(df_cl[['label', 'nb_trips', 'center_distance', 'center_fare']],
                        hide_index=True, use_container_width=True)

    with col2:
        df_pred = query_df("SELECT * FROM batch_fare_predictions ORDER BY borough")
        if not df_pred.empty:
            fig = go.Figure()
            fig.add_trace(go.Bar(x=df_pred['borough'], y=df_pred['avg_actual_fare'],
                                name='Tarif Reel', marker_color='steelblue'))
            fig.add_trace(go.Bar(x=df_pred['borough'], y=df_pred['avg_predicted_fare'],
                                name='Tarif Predit', marker_color='coral'))
            fig.update_layout(title="Prediction Tarif (Regression Lineaire)",
                            barmode='group', height=400)
            st.plotly_chart(fig, use_container_width=True)

            cols = st.columns(2)
            cols[0].metric("R2", f"{df_pred['r2_score'].mean():.3f}")
            cols[1].metric("RMSE", f"{df_pred['rmse'].mean():.2f}")

        df_anom = query_df("SELECT borough, count() as nb_anomalies, "
                          "round(avg(anomaly_score),2) as score_moyen "
                          "FROM batch_anomalies GROUP BY borough ORDER BY nb_anomalies DESC")
        if not df_anom.empty:
            fig = px.bar(df_anom, x='borough', y='nb_anomalies', color='score_moyen',
                        title="Anomalies par Borough (Z-Score)",
                        color_continuous_scale='Reds')
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)


# ───────────────────────────────────────
# TAB 4 : Alertes Live
# ───────────────────────────────────────
with tab4:
    st.subheader("Alertes Temps Reel")
    st.caption("Deviations significatives entre streaming et historique batch (seuil > 30%)")

    df_alerts = query_df("SELECT * FROM stream_alerts ORDER BY alert_time DESC LIMIT 50")
    if not df_alerts.empty:
        cols = st.columns(3)
        cols[0].metric("Total alertes", len(df_alerts))
        cols[1].metric("Boroughs concernes",
                      df_alerts['borough'].nunique() if 'borough' in df_alerts.columns else 0)
        cols[2].metric("Deviation max",
                      f"{df_alerts['deviation_pct'].max():.1f}%" if 'deviation_pct' in df_alerts.columns else "N/A")

        fig = px.scatter(df_alerts, x='alert_time', y='deviation_pct',
                        color='borough', size='deviation_pct',
                        title="Timeline des Alertes",
                        hover_data=['message'])
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)

        st.dataframe(df_alerts, hide_index=True, use_container_width=True)
    else:
        st.info("Aucune alerte. Les alertes apparaitront quand le streaming detectera des deviations > 30% par rapport au batch historique.")


# ───────────────────────────────────────
# TAB 5 : Controle Batch Reprocessing
# ───────────────────────────────────────
with tab5:
    st.subheader("Controle du Batch Reprocessing")

    col_btn, col_info = st.columns([1, 2])

    with col_btn:
        st.markdown("#### Lancement Manuel")
        st.caption("Retraite toutes les donnees streaming accumulees dans HDFS")

        if st.button("Lancer le Batch Reprocessing", type="primary", use_container_width=True):
            try:
                client.command("INSERT INTO batch_trigger VALUES (now())")
                st.success("Batch reprocessing demande ! Le traitement va demarrer dans quelques secondes...")
            except Exception as e:
                st.error(f"Erreur : {e}")

        st.markdown("---")
        st.markdown("#### Planification Automatique")
        st.info("Le batch reprocessing se lance automatiquement **toutes les 4 heures** en plus du declenchement manuel.")

    with col_info:
        st.markdown("#### Dernier Batch")
        df_last = query_df("SELECT * FROM reprocess_log ORDER BY cycle_num DESC LIMIT 1")
        if not df_last.empty:
            last = df_last.iloc[0]
            cols = st.columns(4)
            cols[0].metric("Cycle", f"#{int(last['cycle_num'])}")
            cols[1].metric("Lignes traitees", f"{int(last['rows_processed']):,}")
            cols[2].metric("Duree", f"{last['duration_seconds']:.1f}s")
            cols[3].metric("Execute a", str(last['executed_at'])[11:19])
        else:
            st.info("Aucun batch execute pour le moment.")

    # Trigger en attente ?
    df_pending = query_df("SELECT count() as cnt FROM batch_trigger")
    if not df_pending.empty and df_pending.iloc[0]['cnt'] > 0:
        st.warning("Un batch reprocessing est en attente de traitement...")

    # Historique des cycles
    st.markdown("---")
    df_log = query_df("SELECT * FROM reprocess_log ORDER BY cycle_num")
    if not df_log.empty:
        st.markdown("#### Historique des Cycles")

        col1, col2 = st.columns(2)
        with col1:
            fig = px.line(df_log, x='cycle_num', y='rows_processed',
                         title="Donnees traitees par cycle",
                         markers=True)
            fig.update_layout(height=300, xaxis_title="Cycle", yaxis_title="Lignes")
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig = px.bar(df_log, x='cycle_num', y='duration_seconds',
                        title="Duree par cycle",
                        color='duration_seconds', color_continuous_scale='Oranges')
            fig.update_layout(height=300, xaxis_title="Cycle", yaxis_title="Secondes")
            st.plotly_chart(fig, use_container_width=True)

        st.dataframe(df_log, hide_index=True, use_container_width=True)


# ── Sidebar info ──
with st.sidebar:
    st.header("Architecture Lambda")
    st.markdown("""
    **Batch Layer** (Jan-Nov)
    - Spark distribue sur 2 workers
    - 5 agregations + 3 modeles ML
    - Resultats dans ClickHouse

    **Speed Layer** (Decembre)
    - Kafka + Spark Streaming
    - Stats live dans ClickHouse
    - Detection d'anomalies temps reel

    **Batch Reprocessing**
    - Manuel (bouton) ou auto (4h)
    - Relit TOUT depuis HDFS
    - Resultats exacts vs streaming

    **Serving Layer**
    - ClickHouse (OLAP colonnaire)
    - Dashboard Streamlit (auto-refresh 3s)

    ---
    **Services**
    - HDFS : `localhost:9870`
    - Spark : `localhost:8080`
    - Jupyter : `localhost:8888`
    - ClickHouse : `localhost:8123`
    """)

# ── Auto-refresh (2 secondes) ──
time.sleep(2)
st.rerun()
