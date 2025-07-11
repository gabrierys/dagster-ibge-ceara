import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import json

def load_population_data(csv_path="populacao_municipios_ceara.csv"):
    if not Path(csv_path).exists():
        raise FileNotFoundError(f"Arquivo {csv_path} não encontrado")
    
    df = pd.read_csv(csv_path)
    return df

def create_population_dashboard(df):
    
    plt.style.use('seaborn-v0_8')
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Dashboard Populacional - Municípios do Ceará', fontsize=16, fontweight='bold')

    top_10 = df.nlargest(10, 'populacao')
    axes[0,0].barh(top_10['municipio'].str.replace(' - CE', ''), top_10['populacao'])
    axes[0,0].set_title('Top 10 Municípios Mais Populosos')
    axes[0,0].set_xlabel('População')
    axes[0,0].tick_params(axis='y', labelsize=8)

    axes[0,1].hist(df['populacao'], bins=30, alpha=0.7, color='skyblue', edgecolor='black')
    axes[0,1].set_title('Distribuição da População Municipal')
    axes[0,1].set_xlabel('População')
    axes[0,1].set_ylabel('Número de Municípios')
    axes[0,1].axvline(df['populacao'].mean(), color='red', linestyle='--', label='Média')
    axes[0,1].legend()

    axes[1,0].boxplot(df['populacao'])
    axes[1,0].set_title('Box Plot - População Municipal')
    axes[1,0].set_ylabel('População')
    axes[1,0].tick_params(axis='x', labelbottom=False)

    stats_text = (
        f"ESTATÍSTICAS RESUMIDAS\n\n"
        f"Total de Municípios: {len(df):,}\n"
        f"População Total: {df['populacao'].sum():,}\n\n"
        f"Média: {df['populacao'].mean():,.0f}\n"
        f"Mediana: {df['populacao'].median():,.0f}\n\n"
        f"Maior: {df['populacao'].max():,} ({df.loc[df['populacao'].idxmax(), 'municipio']})\n"
        f"Menor: {df['populacao'].min():,} ({df.loc[df['populacao'].idxmin(), 'municipio']})\n\n"
        f"Desvio Padrão: {df['populacao'].std():,.0f}"
    )
    axes[1,1].text(0.05, 0.95, stats_text, transform=axes[1,1].transAxes, 
                   fontsize=10, verticalalignment='top', fontfamily='monospace')
    axes[1,1].set_xlim(0, 1)
    axes[1,1].set_ylim(0, 1)
    axes[1,1].axis('off')

    plt.tight_layout()
    return fig

def create_top_bottom_comparison(df, n=10):
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
    
    # Top N maiores
    top_n = df.nlargest(n, 'populacao')
    ax1.barh(range(len(top_n)), top_n['populacao'], color='green', alpha=0.7)
    ax1.set_yticks(range(len(top_n)))
    ax1.set_yticklabels(top_n['municipio'].str.replace(' - CE', ''), fontsize=8)
    ax1.set_title(f'Top {n} Maiores Municípios')
    ax1.set_xlabel('População')
    
    # Top N menores
    bottom_n = df.nsmallest(n, 'populacao')
    ax2.barh(range(len(bottom_n)), bottom_n['populacao'], color='red', alpha=0.7)
    ax2.set_yticks(range(len(bottom_n)))
    ax2.set_yticklabels(bottom_n['municipio'].str.replace(' - CE', ''), fontsize=8)
    ax2.set_title(f'Top {n} Menores Municípios')
    ax2.set_xlabel('População')
    
    plt.tight_layout()
    return fig

def export_summary_stats(df, output_file="ceara_stats_summary.json"):
    
    stats = {
        "resumo_geral": {
            "total_municipios": int(len(df)),
            "populacao_total": int(df['populacao'].sum()),
            "populacao_media": float(df['populacao'].mean()),
            "populacao_mediana": float(df['populacao'].median()),
            "desvio_padrao": float(df['populacao'].std())
        },
        "extremos": {
            "maior_municipio": {
                "nome": df.loc[df['populacao'].idxmax(), 'municipio'],
                "populacao": int(df['populacao'].max())
            },
            "menor_municipio": {
                "nome": df.loc[df['populacao'].idxmin(), 'municipio'], 
                "populacao": int(df['populacao'].min())
            }
        },
        "top_10_maiores": [
            {
                "municipio": row['municipio'],
                "populacao": int(row['populacao']),
                "percentual_estado": float(row['populacao'] / df['populacao'].sum() * 100)
            }
            for _, row in df.nlargest(10, 'populacao').iterrows()
        ],
        "distribuicao": {
            "q1": float(df['populacao'].quantile(0.25)),
            "q2": float(df['populacao'].quantile(0.50)),
            "q3": float(df['populacao'].quantile(0.75)),
            "iqr": float(df['populacao'].quantile(0.75) - df['populacao'].quantile(0.25))
        }
    }
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)
    
    print(f"Estatísticas exportadas para: {output_file}")
    return stats

def main():
    
    try:
        print("Carregando dados populacionais...")
        df = load_population_data()
        print(f"Dados carregados: {len(df)} municípios")

        dashboard_fig = create_population_dashboard(df)
        dashboard_fig.savefig('dashboard_populacao_ceara.png', dpi=300, bbox_inches='tight')
        print("Dashboard salvo: dashboard_populacao_ceara.png")

        comparison_fig = create_top_bottom_comparison(df)
        comparison_fig.savefig('comparacao_municipios_ceara.png', dpi=300, bbox_inches='tight')
        print("Comparação salva: comparacao_municipios_ceara.png")

        stats = export_summary_stats(df)

        print("\n=== RESUMO EXECUTIVO ===")
        print(f"Total de Municípios: {stats['resumo_geral']['total_municipios']:,}")
        print(f"População Total: {stats['resumo_geral']['populacao_total']:,}")
        print(f"População Média: {stats['resumo_geral']['populacao_media']:,.0f}")
        print(f"Maior Município: {stats['extremos']['maior_municipio']['nome']} ({stats['extremos']['maior_municipio']['populacao']:,})")
        print(f"Menor Município: {stats['extremos']['menor_municipio']['nome']} ({stats['extremos']['menor_municipio']['populacao']:,})")

        print("\nVisualizações criadas com sucesso.")

    except FileNotFoundError as e:
        print(f"Erro: {e}")
        print("Execute primeiro o pipeline do Dagster para gerar os dados.")
    except Exception as e:
        print(f"Erro inesperado: {e}")

if __name__ == "__main__":
    main()
