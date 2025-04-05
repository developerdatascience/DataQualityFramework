import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime
from typing import Optional
from engine import DataQualityEngine

class HTMLReportGenerator:
    def __init__(self, dq_engine: DataQualityEngine):
        """
        Initialize HTML Report Generator with Data Quality Engine.
        
        :param dq_engine: DataQualityEngine instance with validation results
        """
        self.dq_engine = dq_engine
        self.stats = dq_engine.get_quality_stats()
        self.report_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _generate_donut_chart(self) -> str:
        """Generate donut chart showing passed/rejected records distribution."""
        labels = ['Passed', 'Rejected']
        values = [self.stats['passed_records'], self.stats['rejected_records']]
        
        fig = go.Figure(data=[go.Pie(
            labels=labels,
            values=values,
            hole=.5,
            marker_colors=['#28a745', '#dc3545']
        )])
        fig.update_layout(
            title='Record Distribution',
            showlegend=False,
            annotations=[dict(
                text=f"Total<br>{sum(values):,}",
                x=0.5, y=0.5, 
                font_size=20,
                showarrow=False
            )]
        )
        return fig.to_html(full_html=False)

    def _generate_rules_violation_barchart(self) -> str:
        """Generate horizontal bar chart showing rule violations."""
        rules_data = sorted(
            [(k, v['failed_count']) for k, v in self.stats['rule_violations'].items()],
            key=lambda x: x[1],
            reverse=True
        )
        df = pd.DataFrame(rules_data, columns=['Rule', 'Violations'])
        
        fig = px.bar(
            df,
            x='Violations',
            y='Rule',
            orientation='h',
            title='Rule Violations Count',
            color='Violations',
            color_continuous_scale='Reds'
        )
        fig.update_layout(yaxis={'categoryorder': 'total ascending'})
        return fig.to_html(full_html=False)

    def _generate_metrics_section(self) -> str:
        """Generate summary metrics cards."""
        metrics = [
            ('Total Records', self.stats['total_records'], 'primary'),
            ('Passed Records', self.stats['passed_records'], 'success'),
            ('Rejected Records', self.stats['rejected_records'], 'danger'),
            ('Data Quality %', f"{self.stats['quality_percentage']:.2f}%", 'info')
        ]
        
        cards = []
        for title, value, style in metrics:
            card = f"""
            <div class="col-md-3 mb-4">
                <div class="card border-{style} shadow">
                    <div class="card-body">
                        <h5 class="card-title text-{style}">{title}</h5>
                        <h2 class="card-text">{value}</h2>
                    </div>
                </div>
            </div>
            """
            cards.append(card)
        return '\n'.join(cards)

    def _generate_worst_rules_table(self) -> str:
        """Generate table of top 5 worst performing rules."""
        rules_data = sorted(
            self.stats['rule_violations'].items(),
            key=lambda x: x[1]['failed_count'],
            reverse=True
        )[:5]
        
        rows = []
        for rule, data in rules_data:
            rows.append(f"""
            <tr>
                <td>{rule}</td>
                <td>{data['failed_count']:,}</td>
            </tr>
            """)
        
        return """
        <div class="col-md-6">
            <div class="card shadow">
                <div class="card-header bg-warning text-white">
                    <h5>Top 5 Rule Violations</h5>
                </div>
                <div class="card-body">
                    <table class="table table-hover">
                        <thead>
                            <tr>
                                <th>Rule Name</th>
                                <th>Violations</th>
                            </tr>
                        </thead>
                        <tbody>
                            """ + '\n'.join(rows) + """
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
        """

    def _generate_failed_records_sample(self) -> str:
        """Generate sample table of failed records."""
        sample_df = self.dq_engine.get_failed_rules_summary().limit(10).toPandas()
        rows = []
        for _, row in sample_df.iterrows():
            rules = '<br>'.join(row['failed_rules'])
            rows.append(f"""
            <tr>
                <td>{row['order_id']}</td>
                <td>{rules}</td>
            </tr>
            """)
        
        return """
        <div class="col-md-6">
            <div class="card shadow">
                <div class="card-header bg-danger text-white">
                    <h5>Sample Rejected Records</h5>
                </div>
                <div class="card-body">
                    <table class="table table-hover">
                        <thead>
                            <tr>
                                <th>Order ID</th>
                                <th>Failed Rules</th>
                            </tr>
                        </thead>
                        <tbody>
                            """ + '\n'.join(rows) + """
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
        """ if not sample_df.empty else ""

    def generate_report(self, output_path: str, title: Optional[str] = "Data Quality Report"):
        """
        Generate and save HTML report with visualizations.
        
        :param output_path: Path to save HTML report
        :param title: Report title (default: "Data Quality Report")
        """
        template = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>{title}</title>
            <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
            <style>
                .card {{ margin-bottom: 20px; }}
                .report-header {{ 
                    border-bottom: 2px solid #dee2e6;
                    margin-bottom: 30px;
                    padding-bottom: 20px;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="report-header">
                    <h1 class="display-4">{title}</h1>
                    <p class="lead">Generated on: {self.report_date}</p>
                </div>
                
                <!-- Metrics Section -->
                <div class="row">
                    {self._generate_metrics_section()}
                </div>
                
                <!-- Charts Section -->
                <div class="row">
                    <div class="col-md-6">
                        <div class="card shadow">
                            <div class="card-body">
                                {self._generate_donut_chart()}
                            </div>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="card shadow">
                            <div class="card-body">
                                {self._generate_rules_violation_barchart()}
                            </div>
                        </div>
                    </div>
                </div>
                
                <!-- Tables Section -->
                <div class="row">
                    {self._generate_worst_rules_table()}
                    {self._generate_failed_records_sample()}
                </div>
            </div>
        </body>
        </html>
        """

        with open(output_path, 'w') as f:
            f.write(template)