"""
NYC Taxi - Lambda Architecture Dashboard
"""

import time
import datetime
import streamlit as st
import clickhouse_connect
import plotly.graph_objects as go
import pandas as pd

# ── Palette ──
C_STREAM = '#d94f3b'
C_BATCH = '#2b6e8a'
C_ACCENT = '#e8a838'
C_GREEN = '#5ba368'
C_BOROUGHS = ['#2b6e8a', '#d94f3b', '#e8a838', '#5ba368', '#8c6bb1', '#888']

_FONT = dict(family='Helvetica, Arial, sans-serif', size=12, color='#333')
_MARGIN = dict(l=50, r=30, t=45, b=45)

def style(fig, h=340):
    fig.update_layout(
        font=_FONT, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
        margin=_MARGIN, height=h, title_font_size=14, showlegend=False,
    )
    fig.update_xaxes(showgrid=False, zeroline=False)
    fig.update_yaxes(showgrid=True, gridcolor='#eee', zeroline=False)
    return fig

# ── Page ──
st.set_page_config(page_title="NYC Taxi - Lambda Dashboard", layout="wide",
                   initial_sidebar_state="collapsed")
st.title("NYC Taxi - Architecture Lambda")
st.caption(f"Derniere mise a jour : {datetime.datetime.now().strftime('%H:%M:%S')}")

# ── ClickHouse ──
def get_client():
    return clickhouse_connect.get_client(host='clickhouse', port=8123)

try:
    client = get_client()
except Exception as e:
    st.error(f"Connexion ClickHouse echouee : {e}")
    st.stop()

def query_df(sql):
    try:
        r = client.query(sql)
        return pd.DataFrame(r.result_rows, columns=r.column_names) if r.result_rows else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Streaming vs Batch", "Analyse Historique", "ML Insights", "Alertes Live", "Controle Batch"
])


# ─────────────────────────────────────────────────
# TAB 1 — Streaming vs Batch
# ─────────────────────────────────────────────────
with tab1:
    col_stream, col_batch = st.columns(2)

    with col_stream:
        st.markdown("##### Streaming — temps reel")
        df_s = query_df("SELECT * FROM stream_borough_stats FINAL ORDER BY nb_trips DESC")

        if not df_s.empty:
            total_trips = int(df_s['nb_trips'].sum())
            total_rev = float(df_s['total_revenue'].sum())
            prev_trips = st.session_state.get('prev_trips', total_trips)
            prev_rev = st.session_state.get('prev_rev', total_rev)
            st.session_state['prev_trips'] = total_trips
            st.session_state['prev_rev'] = total_rev

            if 'trip_history' not in st.session_state:
                st.session_state['trip_history'] = []
            st.session_state['trip_history'].append({
                'time': datetime.datetime.now().strftime('%H:%M:%S'),
                'trips': total_trips, 'revenue': total_rev
            })
            if len(st.session_state['trip_history']) > 60:
                st.session_state['trip_history'] = st.session_state['trip_history'][-60:]

            c1, c2, c3 = st.columns(3)
            d_t = total_trips - prev_trips
            d_r = total_rev - prev_rev
            c1.metric("Trajets", f"{total_trips:,}", delta=f"+{d_t:,}" if d_t > 0 else None)
            c2.metric("Revenu", f"${total_rev:,.0f}", delta=f"+${d_r:,.0f}" if d_r > 0 else None)
            c3.metric("Tarif moy.", f"${df_s['avg_fare'].mean():.2f}")

            # ── Live curve with value annotations ──
            hist = pd.DataFrame(st.session_state['trip_history'])
            if len(hist) > 1:
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=hist['time'], y=hist['trips'],
                    mode='lines+markers+text', fill='tozeroy',
                    line=dict(color=C_STREAM, width=2),
                    fillcolor='rgba(217,79,59,0.08)',
                    marker=dict(size=4, color=C_STREAM),
                    text=[f'{v:,}' if i % 5 == 0 or i == len(hist)-1 else ''
                          for i, v in enumerate(hist['trips'])],
                    textposition='top center', textfont=dict(size=9, color='#666'),
                ))
                style(fig, h=230)
                fig.update_layout(title='Trajets cumules au fil du temps',
                                  xaxis_title='Heure', yaxis_title='Trajets')
                st.plotly_chart(fig, use_container_width=True)

            # ── Borough bars with values inside ──
            fig = go.Figure(go.Bar(
                x=df_s['borough'], y=df_s['nb_trips'],
                marker_color=C_STREAM, marker_line=dict(width=0),
                text=df_s['nb_trips'].apply(lambda x: f'{x:,}'),
                textposition='outside', textfont=dict(size=11),
                width=0.55,
            ))
            style(fig, h=300)
            fig.update_layout(title='Trajets par borough',
                              yaxis_title='Nb de trajets')
            st.plotly_chart(fig, use_container_width=True)

            # ── Revenue per borough ──
            fig = go.Figure(go.Bar(
                x=df_s['borough'], y=df_s['total_revenue'],
                marker_color=C_STREAM, marker_opacity=0.7,
                text=df_s['total_revenue'].apply(lambda x: f'${x:,.0f}'),
                textposition='outside', textfont=dict(size=10),
                width=0.55,
            ))
            style(fig, h=260)
            fig.update_layout(title='Revenu par borough', yaxis_title='Revenu ($)')
            st.plotly_chart(fig, use_container_width=True)

        else:
            st.info("En attente de donnees streaming...")

        # ── Top zones ──
        df_tz = query_df("SELECT * FROM stream_top_zones FINAL ORDER BY nb_trips DESC LIMIT 8")
        if not df_tz.empty:
            fig = go.Figure(go.Bar(
                y=df_tz['zone'], x=df_tz['nb_trips'], orientation='h',
                marker_color=C_STREAM,
                text=df_tz['nb_trips'].apply(lambda x: f'{x:,}'),
                textposition='outside', textfont=dict(size=10),
            ))
            style(fig, h=280)
            fig.update_layout(title='Top zones de depart',
                              xaxis_title='Nb de trajets',
                              yaxis=dict(autorange='reversed'))
            st.plotly_chart(fig, use_container_width=True)

    # ── BATCH COLUMN ──
    with col_batch:
        st.markdown("##### Batch Reprocessing — calcul exact")
        df_r = query_df("SELECT * FROM reprocess_borough_stats ORDER BY nb_trips DESC")

        if not df_r.empty:
            c1, c2, c3 = st.columns(3)
            c1.metric("Trajets", f"{df_r['nb_trips'].sum():,.0f}")
            c2.metric("Revenu", f"${df_r['total_revenue'].sum():,.0f}")
            c3.metric("Dist. moy.", f"{df_r['avg_distance'].mean():.1f} mi")

            fig = go.Figure(go.Bar(
                x=df_r['borough'], y=df_r['nb_trips'],
                marker_color=C_BATCH,
                text=df_r['nb_trips'].apply(lambda x: f'{x:,}'),
                textposition='outside', textfont=dict(size=11),
                width=0.55,
            ))
            style(fig, h=300)
            fig.update_layout(title='Trajets par borough', yaxis_title='Nb de trajets')
            st.plotly_chart(fig, use_container_width=True)

            fig2 = go.Figure(go.Bar(
                x=df_r['borough'], y=df_r['total_revenue'],
                marker_color=C_BATCH, marker_opacity=0.7,
                text=df_r['total_revenue'].apply(lambda x: f'${x:,.0f}'),
                textposition='outside', textfont=dict(size=10),
                width=0.55,
            ))
            style(fig2, h=260)
            fig2.update_layout(title='Revenu par borough', yaxis_title='Revenu ($)')
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("Aucun batch lance. Utilisez l'onglet Controle Batch.")

    # ── Comparison ──
    if not df_s.empty and not df_r.empty:
        st.markdown("---")
        st.markdown("##### Comparaison streaming / batch")
        merged = df_s[['borough','nb_trips']].merge(
            df_r[['borough','nb_trips']], on='borough',
            suffixes=('_stream','_batch'), how='outer').fillna(0)

        fig = go.Figure()
        fig.add_trace(go.Bar(x=merged['borough'], y=merged['nb_trips_stream'],
                             name='Streaming', marker_color=C_STREAM,
                             text=merged['nb_trips_stream'].apply(lambda x: f'{int(x):,}'),
                             textposition='outside', textfont=dict(size=10)))
        fig.add_trace(go.Bar(x=merged['borough'], y=merged['nb_trips_batch'],
                             name='Batch', marker_color=C_BATCH,
                             text=merged['nb_trips_batch'].apply(lambda x: f'{int(x):,}'),
                             textposition='outside', textfont=dict(size=10)))
        fig.update_layout(
            font=_FONT, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
            margin=_MARGIN, height=340, barmode='group', title_font_size=14,
            showlegend=True, legend=dict(orientation='h', y=1.12, x=0.5, xanchor='center'),
            yaxis_title='Nb de trajets',
        )
        fig.update_xaxes(showgrid=False, zeroline=False)
        fig.update_yaxes(showgrid=True, gridcolor='#eee', zeroline=False)
        st.plotly_chart(fig, use_container_width=True)


# ─────────────────────────────────────────────────
# TAB 2 — Analyse Historique
# ─────────────────────────────────────────────────
with tab2:
    st.markdown("##### Donnees batch — Janvier a Novembre 2023")

    df = query_df("SELECT * FROM batch_stats_borough ORDER BY nb_trips DESC")
    if not df.empty:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Trajets", f"{df['nb_trips'].sum():,.0f}")
        c2.metric("Revenu total", f"${df['total_revenue'].sum():,.0f}")
        c3.metric("Dist. moyenne", f"{df['avg_distance'].mean():.1f} mi")
        c4.metric("Boroughs", f"{len(df)}")

    col1, col2 = st.columns(2)

    with col1:
        # ── Borough bars with % share ──
        if not df.empty:
            total = df['nb_trips'].sum()
            fig = go.Figure(go.Bar(
                x=df['borough'], y=df['nb_trips'],
                marker_color=C_BOROUGHS[:len(df)],
                text=df.apply(lambda r: f"{r['nb_trips']:,}<br><span style='font-size:9px;color:#888'>{r['nb_trips']/total*100:.1f}%</span>", axis=1),
                textposition='outside',
                width=0.6,
            ))
            style(fig, h=370)
            fig.update_layout(title='Repartition des trajets par borough',
                              yaxis_title='Nb de trajets')
            st.plotly_chart(fig, use_container_width=True)

        df_zones = query_df("SELECT * FROM batch_top_zones ORDER BY nb_trips DESC LIMIT 10")
        if not df_zones.empty:
            fig = go.Figure(go.Bar(
                y=df_zones['zone'], x=df_zones['nb_trips'], orientation='h',
                marker_color=C_BATCH,
                text=df_zones['nb_trips'].apply(lambda x: f'{x:,}'),
                textposition='outside', textfont=dict(size=10),
            ))
            style(fig, h=360)
            fig.update_layout(title='Top 10 zones de pickup',
                              xaxis_title='Nb de trajets',
                              yaxis=dict(autorange='reversed'))
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        df_month = query_df("SELECT * FROM batch_stats_monthly ORDER BY year, month")
        if not df_month.empty:
            noms = {1:'Jan',2:'Fev',3:'Mar',4:'Avr',5:'Mai',6:'Jun',
                    7:'Jul',8:'Aou',9:'Sep',10:'Oct',11:'Nov',12:'Dec'}
            df_month['mois'] = df_month['month'].map(noms)

            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=df_month['mois'], y=df_month['nb_trips'],
                name='Trajets', marker_color=C_BATCH,
                text=df_month['nb_trips'].apply(lambda x: f'{x:,}'),
                textposition='outside', textfont=dict(size=9),
            ))
            fig.add_trace(go.Scatter(
                x=df_month['mois'], y=df_month['total_revenue'],
                name='Revenu ($)', yaxis='y2',
                line=dict(color=C_ACCENT, width=2),
                mode='lines+markers', marker=dict(size=5),
            ))
            fig.update_layout(
                font=_FONT, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                margin=_MARGIN, height=370, title='Evolution mensuelle', title_font_size=14,
                showlegend=True, legend=dict(orientation='h', y=1.12, x=0.5, xanchor='center'),
                yaxis=dict(title='Trajets', showgrid=True, gridcolor='#eee', zeroline=False),
                yaxis2=dict(title='Revenu ($)', overlaying='y', side='right', showgrid=False, zeroline=False),
            )
            fig.update_xaxes(showgrid=False, zeroline=False)
            st.plotly_chart(fig, use_container_width=True)

        # ── Routes: origin > destination with fare annotation ──
        df_od = query_df("SELECT * FROM batch_od_matrix ORDER BY nb_trips DESC LIMIT 10")
        if not df_od.empty:
            df_od['route'] = df_od['zone_pickup'] + '  >  ' + df_od['zone_dropoff']
            fig = go.Figure(go.Bar(
                y=df_od['route'], x=df_od['nb_trips'], orientation='h',
                marker_color=C_ACCENT,
                text=df_od.apply(
                    lambda r: f"{r['nb_trips']:,} trajets  |  tarif moy. ${r['avg_fare']:.0f}", axis=1),
                textposition='outside', textfont=dict(size=10),
            ))
            style(fig, h=400)
            fig.update_layout(title='Top 10 routes (depart > arrivee)',
                              xaxis_title='Nb de trajets',
                              yaxis=dict(autorange='reversed'))
            fig.update_xaxes(showgrid=True, gridcolor='#eee')
            st.plotly_chart(fig, use_container_width=True)

    # ── Bottom row: meteo + jours ──
    col3, col4 = st.columns(2)

    with col3:
        df_meteo = query_df("SELECT * FROM batch_stats_weather ORDER BY nb_trips DESC")
        if not df_meteo.empty:
            # Bin temperature into ranges for a readable bar chart
            bins = [(-999, 5), (5, 10), (10, 15), (15, 20), (20, 25), (25, 35)]
            labels = ['< 5 C', '5-10 C', '10-15 C', '15-20 C', '20-25 C', '> 25 C']
            records = []
            for (lo, hi), lab in zip(bins, labels):
                subset = df_meteo[(df_meteo['temp_max'] > lo) & (df_meteo['temp_max'] <= hi)]
                if not subset.empty:
                    records.append({
                        'range': lab,
                        'nb_trips': int(subset['nb_trips'].sum()),
                        'avg_fare': round(subset['avg_fare'].mean(), 1),
                        'avg_precip': round(subset['precipitation'].mean(), 1),
                    })
            df_bins = pd.DataFrame(records)

            if not df_bins.empty:
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=df_bins['range'], y=df_bins['nb_trips'],
                    marker_color=[C_BATCH, C_BATCH, C_GREEN, C_GREEN, C_ACCENT, C_STREAM][:len(df_bins)],
                    text=df_bins.apply(
                        lambda r: f"{r['nb_trips']:,}<br><span style='font-size:9px;color:#888'>tarif ${r['avg_fare']} | pluie {r['avg_precip']}mm</span>", axis=1),
                    textposition='outside',
                    width=0.6,
                ))
                style(fig, h=380)
                fig.update_layout(title='Activite selon la temperature',
                                  xaxis_title='Plage de temperature',
                                  yaxis_title='Nb de trajets')
                st.plotly_chart(fig, use_container_width=True)

    with col4:
        df_day = query_df("SELECT * FROM batch_day_patterns ORDER BY day_of_week, borough")
        if not df_day.empty:
            noms_j = {1:'Lun',2:'Mar',3:'Mer',4:'Jeu',5:'Ven',6:'Sam',7:'Dim'}
            df_day['jour'] = df_day['day_of_week'].map(noms_j)
            piv = df_day.groupby('jour', sort=False)['nb_trips'].sum().reset_index()
            ordre = ['Lun','Mar','Mer','Jeu','Ven','Sam','Dim']
            piv['jour'] = pd.Categorical(piv['jour'], categories=ordre, ordered=True)
            piv = piv.sort_values('jour')

            colors = [C_BATCH]*5 + [C_ACCENT]*2
            fig = go.Figure(go.Bar(
                x=piv['jour'], y=piv['nb_trips'],
                marker_color=colors[:len(piv)],
                text=piv['nb_trips'].apply(lambda x: f'{x:,}'),
                textposition='outside', textfont=dict(size=10),
                width=0.55,
            ))
            style(fig, h=380)
            fig.update_layout(title='Activite par jour de la semaine',
                              yaxis_title='Nb de trajets')
            st.plotly_chart(fig, use_container_width=True)


# ─────────────────────────────────────────────────
# TAB 3 — ML Insights
# ─────────────────────────────────────────────────
with tab3:
    st.markdown("##### Resultats des modeles SparkML")
    col1, col2 = st.columns(2)

    with col1:
        df_cl = query_df("SELECT * FROM batch_clusters ORDER BY cluster_id")
        if not df_cl.empty:
            fig = go.Figure(go.Scatter(
                x=df_cl['center_distance'], y=df_cl['center_fare'],
                mode='markers+text',
                marker=dict(
                    size=df_cl['nb_trips'] / df_cl['nb_trips'].max() * 50 + 12,
                    color=C_BOROUGHS[:len(df_cl)],
                    line=dict(width=1, color='#fff'),
                ),
                text=df_cl['label'], textposition='top center', textfont=dict(size=10),
            ))
            style(fig, h=380)
            fig.update_layout(title='Clusters K-Means',
                              xaxis_title='Distance moy. (mi)', yaxis_title='Tarif moy. ($)')
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(
                df_cl[['label','nb_trips','center_distance','center_fare']].rename(columns={
                    'label':'Cluster','nb_trips':'Trajets','center_distance':'Distance (mi)','center_fare':'Tarif ($)'
                }), hide_index=True, use_container_width=True)

    with col2:
        df_pred = query_df("SELECT * FROM batch_fare_predictions ORDER BY borough")
        if not df_pred.empty:
            fig = go.Figure()
            fig.add_trace(go.Bar(x=df_pred['borough'], y=df_pred['avg_actual_fare'],
                                 name='Reel', marker_color=C_BATCH))
            fig.add_trace(go.Bar(x=df_pred['borough'], y=df_pred['avg_predicted_fare'],
                                 name='Predit', marker_color=C_ACCENT))
            fig.update_layout(
                font=_FONT, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                margin=_MARGIN, height=380, barmode='group', title_font_size=14,
                title='Tarif reel vs predit (Regression lineaire)',
                showlegend=True, legend=dict(orientation='h', y=1.12, x=0.5, xanchor='center'),
                yaxis_title='Tarif ($)',
            )
            fig.update_xaxes(showgrid=False, zeroline=False)
            fig.update_yaxes(showgrid=True, gridcolor='#eee', zeroline=False)
            st.plotly_chart(fig, use_container_width=True)

            c1, c2 = st.columns(2)
            c1.metric("R2", f"{df_pred['r2_score'].mean():.3f}")
            c2.metric("RMSE", f"{df_pred['rmse'].mean():.2f}")

        df_anom = query_df("SELECT borough, count() as nb_anomalies, "
                           "round(avg(anomaly_score),2) as score_moyen "
                           "FROM batch_anomalies GROUP BY borough ORDER BY nb_anomalies DESC")
        if not df_anom.empty:
            fig = go.Figure(go.Bar(
                x=df_anom['borough'], y=df_anom['nb_anomalies'],
                marker=dict(color=df_anom['score_moyen'],
                            colorscale=[[0,C_ACCENT],[1,C_STREAM]],
                            colorbar=dict(title='Score', thickness=12, len=0.6)),
                text=df_anom['nb_anomalies'].apply(lambda x: f'{x:,}'),
                textposition='outside',
            ))
            style(fig, h=320)
            fig.update_layout(title='Anomalies par borough (Z-Score)', yaxis_title='Nb anomalies')
            st.plotly_chart(fig, use_container_width=True)


# ─────────────────────────────────────────────────
# TAB 4 — Alertes Live
# ─────────────────────────────────────────────────
with tab4:
    st.markdown("##### Alertes — ecarts streaming vs historique (seuil 30%)")
    df_alerts = query_df("SELECT * FROM stream_alerts ORDER BY alert_time DESC LIMIT 50")

    if not df_alerts.empty:
        c1, c2, c3 = st.columns(3)
        c1.metric("Alertes", len(df_alerts))
        c2.metric("Boroughs touches", df_alerts['borough'].nunique() if 'borough' in df_alerts.columns else 0)
        c3.metric("Ecart max",
                  f"{df_alerts['deviation_pct'].max():.1f}%" if 'deviation_pct' in df_alerts.columns else "—")

        fig = go.Figure(go.Scatter(
            x=df_alerts['alert_time'], y=df_alerts['deviation_pct'],
            mode='markers',
            marker=dict(size=10, color=df_alerts['deviation_pct'],
                        colorscale=[[0,C_ACCENT],[1,C_STREAM]],
                        line=dict(width=0.5, color='#fff')),
            text=df_alerts.apply(lambda r: f"{r['borough']}: {r.get('message','')}", axis=1),
            hoverinfo='text+y',
        ))
        style(fig, h=320)
        fig.update_layout(title='Chronologie des alertes',
                          xaxis_title='Heure', yaxis_title='Ecart (%)')
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(df_alerts, hide_index=True, use_container_width=True)
    else:
        st.info("Aucune alerte pour le moment.")


# ─────────────────────────────────────────────────
# TAB 5 — Controle Batch
# ─────────────────────────────────────────────────
with tab5:
    st.markdown("##### Controle du batch reprocessing")
    col_btn, col_info = st.columns([1, 2])

    with col_btn:
        st.markdown("###### Lancement manuel")
        st.caption("Absorbe les donnees streaming dans le batch et repart de zero")
        if st.button("Lancer le Batch Reprocessing", type="primary", use_container_width=True):
            try:
                client.command("INSERT INTO batch_trigger VALUES (now())")
                st.session_state['batch_triggered'] = True
                st.session_state['batch_trigger_time'] = datetime.datetime.now()
                st.session_state['batch_error'] = None
            except Exception as e:
                st.session_state['batch_triggered'] = False
                st.session_state['batch_error'] = str(e)

        if st.session_state.get('batch_error'):
            st.error(f"Erreur : {st.session_state['batch_error']}")
        elif st.session_state.get('batch_triggered'):
            t = st.session_state.get('batch_trigger_time')
            if t and (datetime.datetime.now() - t).total_seconds() < 30:
                st.success("Batch demande — traitement en cours...")
            else:
                st.session_state['batch_triggered'] = False

        st.markdown("---")
        st.caption("Declenchement automatique toutes les 4 heures.")

    with col_info:
        st.markdown("###### Dernier cycle")
        df_last = query_df("SELECT * FROM reprocess_log ORDER BY cycle_num DESC LIMIT 1")
        if not df_last.empty:
            last = df_last.iloc[0]
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Cycle", f"#{int(last['cycle_num'])}")
            c2.metric("Lignes", f"{int(last['rows_processed']):,}")
            c3.metric("Duree", f"{last['duration_seconds']:.1f}s")
            c4.metric("Heure", str(last['executed_at'])[11:19])
        else:
            st.info("Aucun batch execute.")

    df_pending = query_df("SELECT count() as cnt FROM batch_trigger")
    if not df_pending.empty and df_pending.iloc[0]['cnt'] > 0:
        st.warning("Un batch est en attente de traitement...")

    df_log = query_df("SELECT * FROM reprocess_log ORDER BY cycle_num")
    if not df_log.empty:
        st.markdown("---")
        st.markdown("###### Historique des cycles")
        col1, col2 = st.columns(2)
        with col1:
            fig = go.Figure(go.Scatter(
                x=df_log['cycle_num'], y=df_log['rows_processed'],
                mode='lines+markers', line=dict(color=C_BATCH, width=2), marker=dict(size=6),
                text=df_log['rows_processed'].apply(lambda x: f'{x:,}'), hoverinfo='text+x',
            ))
            style(fig, h=260)
            fig.update_layout(title='Lignes traitees', xaxis_title='Cycle', yaxis_title='Lignes')
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            fig = go.Figure(go.Bar(
                x=df_log['cycle_num'], y=df_log['duration_seconds'],
                marker_color=C_ACCENT,
                text=df_log['duration_seconds'].apply(lambda x: f'{x:.1f}s'),
                textposition='outside',
            ))
            style(fig, h=260)
            fig.update_layout(title='Duree par cycle', xaxis_title='Cycle', yaxis_title='Secondes')
            st.plotly_chart(fig, use_container_width=True)
        st.dataframe(df_log, hide_index=True, use_container_width=True)


# ── Sidebar ──
with st.sidebar:
    st.markdown("### Architecture Lambda")
    st.markdown("""
**Batch** (Jan-Nov)
Spark sur 2 workers, 5 agregations + 3 modeles ML, resultats dans ClickHouse.

**Streaming** (Decembre)
Kafka + Spark Streaming, stats live dans ClickHouse, detection d'anomalies.

**Batch Reprocessing**
Manuel ou auto (4h). Absorbe les donnees streaming dans le batch et repart de zero.

**Serving**
ClickHouse + Streamlit.
    """)
    st.markdown("---")
    st.caption("HDFS :9870 | Spark :8080 | Jupyter :8888 | ClickHouse :8123")

time.sleep(2)
st.rerun()
