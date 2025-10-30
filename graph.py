import os
import glob
import pandas as pd
import matplotlib.pyplot as plt

ANALYSIS_DIR = 'analysis'

def plot_cross_summaries():
    """Génère un graphique PNG de l'évolution du nombre de paires uniques par année."""
    pattern = os.path.join(ANALYSIS_DIR, 'cross_summary*.csv')
    files = glob.glob(pattern)
    if not files:
        print("Aucun fichier cross_summary*.csv trouvé dans le dossier analysis/.")
        return

    print(f"{len(files)} fichiers trouvés :")
    for f in files:
        print("  -", os.path.basename(f))

    plt.figure(figsize=(8, 5))
    any_data = False

    for f in sorted(files):
        base = os.path.basename(f)
        match = re.search(r'_min(\d+)', base)
        label = f"min≥{match.group(1)}" if match else "Toutes"

        try:
            df = pd.read_csv(f)
        except Exception as e:
            print(f"Erreur de lecture du fichier {base}: {e}")
            continue

        if df.empty:
            print(f"⚠️  Fichier vide : {base}")
            continue
        if not {'Year', 'PairesUniques'}.issubset(df.columns):
            print(f"⚠️  Colonnes manquantes dans {base}")
            print("   Colonnes présentes :", list(df.columns))
            continue

        plt.plot(df['Year'], df['PairesUniques'], marker='o', label=label)
        any_data = True

    if not any_data:
        print("Aucune donnée valide trouvée pour générer le graphique.")
        return

    plt.title("Évolution du nombre de paires uniques par année")
    plt.xlabel("Année")
    plt.ylabel("Nombre de paires uniques")
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.legend(title="Filtre sur nbre de règles", loc="best")
    plt.tight_layout()

    out_png = os.path.join(ANALYSIS_DIR, 'cross_summary_evolution.png')
    plt.savefig(out_png, dpi=150)
    plt.close()

    print(f"✅ Graphique généré → {out_png}")

if __name__ == "__main__":
    plot_cross_summaries()
