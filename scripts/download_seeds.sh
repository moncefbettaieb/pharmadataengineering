#!/bin/bash

# Variables
URL="https://www.google.com/basepages/producttype/taxonomy-with-ids.fr-FR.txt"
OUTPUT_FILE="seeds/taxonomy-with-ids.fr-FR.txt"
TEMP_FILE="seeds/temp_taxonomy-with-ids.fr-FR.txt"

# Étape 1 : Télécharger le fichier
wget -O "$TEMP_FILE" "$URL"

if [ $? -ne 0 ]; then
  echo "Erreur : Échec du téléchargement depuis $URL"
  exit 1
fi

echo "Fichier téléchargé avec succès : $TEMP_FILE"

# Étape 2 : Vérifier l’encodage du fichier
ENCODING=$(file -i "$TEMP_FILE" | awk -F'=' '{print $2}')

echo "Encodage détecté : $ENCODING"

# Étape 3 : Convertir en UTF-8 si nécessaire
if [ "$ENCODING" != "utf-8" ]; then
  echo "Conversion du fichier en UTF-8..."
  iconv -f "$ENCODING" -t utf-8 "$TEMP_FILE" > "$OUTPUT_FILE"
  if [ $? -ne 0 ]; then
    echo "Erreur : Échec de la conversion en UTF-8"
    exit 1
  fi
else
  echo "Aucune conversion nécessaire, le fichier est déjà en UTF-8"
  mv "$TEMP_FILE" "$OUTPUT_FILE"
fi

# Nettoyer les fichiers temporaires
rm -f "$TEMP_FILE"

echo "Fichier final disponible dans : $OUTPUT_FILE"
