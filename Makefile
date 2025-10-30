# Nom du script principal
SCRIPT1 := duplicate_analysis.py
SCRIPT2 := analyze_new_pairs.py
PYTHON := python3
OUT_DIR := out

# Option par défaut : menu interactif
default: menu

# ---- Exécution principale ----
menu:
	@$(PYTHON) $(SCRIPT1)

# ---- Analyse des résultats ----
analyze:
	@$(PYTHON) $(SCRIPT2)

# ---- Nettoyer le dossier de sortie ----
clean:
	@echo "=== Nettoyage du dossier $(OUT_DIR) ==="
	@rm -rf $(OUT_DIR)/*
	@echo "Dossier nettoyé."

# ---- Aide ----
help:
	@echo "Commandes disponibles :"
	@echo "  make                 -> Lancer le menu interactif"
	@echo "  make clean           -> Supprimer le contenu du dossier 'out'"
