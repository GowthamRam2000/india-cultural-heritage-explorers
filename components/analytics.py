import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np


class AnalyticsVisualizer:
    def __init__(self):
        self.color_palette = px.colors.qualitative.Set3

    def create_heritage_index_chart(self, heritage_df):
        fig = px.bar(
            heritage_df.sort_values('heritage_index', ascending=False).head(15),
            x='heritage_index',
            y='state',
            orientation='h',
            color='heritage_index',
            color_continuous_scale='Viridis',
            title='Heritage Index by State (Top 15)',
            labels={'heritage_index': 'Heritage Index Score', 'state': 'State'}
        )

        fig.update_layout(
            height=500,
            showlegend=False,
            xaxis_title='Heritage Index Score',
            yaxis_title='',
            title_font_size=20
        )

        return fig

    def create_art_forms_distribution(self, df_arts):
        category_counts = df_arts['category'].value_counts()

        fig = go.Figure(data=[go.Pie(
            labels=category_counts.index,
            values=category_counts.values,
            hole=0.3,
            marker_colors=self.color_palette
        )])

        fig.update_layout(
            title='Distribution of Traditional Art Forms by Category',
            height=400,
            title_font_size=20
        )

        return fig

    def create_tourism_trends(self, df_tourism):
        monthly_visitors = df_tourism.groupby('date').agg({
            'domestic_visitors': 'sum',
            'international_visitors': 'sum'
        }).reset_index()

        fig = go.Figure()

        fig.add_trace(go.Scatter(
            x=monthly_visitors['date'],
            y=monthly_visitors['domestic_visitors'],
            mode='lines',
            name='Domestic Visitors',
            line=dict(color='blue', width=2)
        ))

        fig.add_trace(go.Scatter(
            x=monthly_visitors['date'],
            y=monthly_visitors['international_visitors'],
            mode='lines',
            name='International Visitors',
            line=dict(color='red', width=2)
        ))

        fig.update_layout(
            title='Tourism Trends Over Time',
            xaxis_title='Date',
            yaxis_title='Number of Visitors',
            height=400,
            hovermode='x unified',
            title_font_size=20
        )

        return fig

    def create_seasonal_patterns(self, seasonal_df):
        fig = px.line(
            seasonal_df,
            x='month',
            y='total_visitors',
            markers=True,
            title='Seasonal Tourism Patterns',
            labels={'month': 'Month', 'total_visitors': 'Average Visitors'}
        )

        fig.update_layout(
            xaxis=dict(
                tickmode='array',
                tickvals=list(range(1, 13)),
                ticktext=['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                          'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
            ),
            height=400,
            title_font_size=20
        )

        return fig

    def create_risk_assessment_chart(self, df_arts):
        risk_counts = df_arts['risk_level'].value_counts()

        fig = go.Figure(data=[
            go.Bar(
                x=risk_counts.index,
                y=risk_counts.values,
                marker_color=['#2ecc71', '#f39c12', '#e74c3c']
            )
        ])

        fig.update_layout(
            title='Art Forms at Risk',
            xaxis_title='Risk Level',
            yaxis_title='Number of Art Forms',
            height=400,
            showlegend=False,
            title_font_size=20
        )

        return fig

    def create_sustainability_matrix(self, sustainability_df):
        fig = px.scatter(
            sustainability_df,
            x='sustainability_score',
            y='crowding_index',
            size='revenue',
            color='overall_sustainability',
            hover_name='site',
            color_continuous_scale='RdYlGn',
            title='Sustainability Matrix of Cultural Sites',
            labels={
                'sustainability_score': 'Sustainability Score',
                'crowding_index': 'Crowding Index',
                'overall_sustainability': 'Overall Sustainability'
            }
        )

        fig.update_layout(
            height=500,
            title_font_size=20
        )

        return fig

    def create_festival_impact_chart(self, df_festivals):
        top_festivals = df_festivals.groupby('festival').agg({
            'economic_impact': 'sum',
            'expected_visitors': 'sum',
            'cultural_significance_score': 'mean'
        }).sort_values('economic_impact', ascending=False).head(10)

        fig = make_subplots(
            rows=1, cols=2,
            subplot_titles=('Economic Impact', 'Cultural Significance'),
            specs=[[{'type': 'bar'}, {'type': 'bar'}]]
        )

        fig.add_trace(
            go.Bar(
                x=top_festivals.index,
                y=top_festivals['economic_impact'],
                name='Economic Impact',
                marker_color='lightblue'
            ),
            row=1, col=1
        )

        fig.add_trace(
            go.Bar(
                x=top_festivals.index,
                y=top_festivals['cultural_significance_score'],
                name='Cultural Significance',
                marker_color='lightcoral'
            ),
            row=1, col=2
        )

        fig.update_xaxes(tickangle=45)
        fig.update_layout(
            height=500,
            showlegend=False,
            title_text='Top 10 Festivals: Economic & Cultural Impact',
            title_font_size=20
        )

        return fig

    def create_digital_presence_chart(self, df_sites):
        digital_scores = df_sites.groupby('state')['digital_presence_score'].mean().sort_values(ascending=False).head(
            15)

        fig = go.Figure(data=[
            go.Bar(
                x=digital_scores.values,
                y=digital_scores.index,
                orientation='h',
                marker=dict(
                    color=digital_scores.values,
                    colorscale='Blues',
                    showscale=True,
                    colorbar=dict(title='Score')
                )
            )
        ])

        fig.update_layout(
            title='Digital Presence Score by State (Top 15)',
            xaxis_title='Average Digital Presence Score',
            yaxis_title='',
            height=500,
            title_font_size=20
        )

        return fig

    def create_hidden_gems_radar(self, hidden_gems):
        categories = ['Accessibility', 'Underutilization', 'Digital Presence', 'Conservation']

        fig = go.Figure()

        for idx, site in hidden_gems.head(5).iterrows():
            values = [
                site['accessibility_score'],
                1 - site['current_utilization'],
                site['digital_presence_score'],
                0.8 if site['conservation_status'] in ['Excellent', 'Good'] else 0.4
            ]

            fig.add_trace(go.Scatterpolar(
                r=values,
                theta=categories,
                fill='toself',
                name=site['site_name'][:30]
            ))

        fig.update_layout(
            polar=dict(
                radialaxis=dict(
                    visible=True,
                    range=[0, 1]
                )
            ),
            showlegend=True,
            title='Hidden Gems Potential Analysis',
            height=500,
            title_font_size=20
        )

        return fig