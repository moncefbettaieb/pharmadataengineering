import re

# Chemins d'entrée/sortie
input_file = "seeds/taxonomy-with-ids.fr-FR.txt"
output_file = "seeds/cleaned_google_product_taxonomy.csv"

# Mapping des caractères spéciaux à corriger
char_mapping = {
    "Ã©": "é",
    "Ã‰": "É",
    "Ã ": "à",
    "Ãª": "ê",
    "Ã¨": "è",
    "Ã»": "û",
    "Ã´": "ô",
    "Ã§": "ç",
    "Ã€": "À",
    "Ã¶": "ö",
    "Ã¼": "ü",
    "Ã§": "ç"
}

# Fonction de nettoyage
def clean_text(line):
    for char, replacement in char_mapping.items():
        line = line.replace(char, replacement)
    return line

# Lecture et nettoyage du fichier
with open(input_file, "r", encoding="utf-8") as infile, open(output_file, "w", encoding="utf-8") as outfile:
    outfile.write("taxonomy_id;taxonomy_name\n")
    for line in infile:
        # Ignore les lignes de commentaires
        if line.startswith("#"):
            continue
        # Nettoie les caractères spéciaux
        cleaned_line = clean_text(line.strip())
        # Extraction des champs
        match = re.match(r"(\d+)\s-\s(.+)", cleaned_line)
        if match:
            taxonomy_id, taxonomy_name = match.groups()
            # Écrit les champs dans le fichier CSV avec ";" comme séparateur
            outfile.write(f"{taxonomy_id};{taxonomy_name}\n")

print("Fichier nettoyé et converti en CSV : ", output_file)
