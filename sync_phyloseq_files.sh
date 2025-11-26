#!/bin/bash

# Script para sincronizar archivos phyloseq
# Uso: bash sync_phyloseq_files.sh

# Archivos de entrada
SAMPLES="meta_metadata_2.csv"
OTU_TABLE="OTU_table_hipogeos.csv"
TAX_TABLE="tax_table_hipogeos.csv"


# Archivos de salida
OTU_OUTPUT="Otu_table_updated.csv"
TAX_OUTPUT="Tax_table_updated.csv"

echo "=== Iniciando sincronización de archivos phyloseq ==="
echo "Samples: $(wc -l < $SAMPLES) líneas"
echo "OTU_table: $(wc -l < $OTU_TABLE) líneas"
echo "Tax_table: $(wc -l < $TAX_TABLE) líneas"

# 1. Extraer Plot y genus de Samples y crear nuevos IDs de OTU
echo -e "\n=== Paso 1: Extrayendo Plot_genus de Samples ==="

# Primero, verificar las columnas
echo "Verificando columnas en Samples.csv..."
head -1 $SAMPLES | tr ',' '\n' | nl | grep -E "Plot|genus"

awk -F',' 'NR==1 {
    plot_idx=0; genus_idx=0
    for(i=1; i<=NF; i++) {
        col = $i
        gsub(/^[ \t]+|[ \t]+$/, "", col)
        gsub(/^"|"$/, "", col)
        # Buscar exactamente "Plot" y "genus"
        if(col=="Plot") {
            plot_idx=i
            print "Columna Plot encontrada en posición: " i > "/dev/stderr"
        }
        if(col=="genus") {
            genus_idx=i
            print "Columna genus encontrada en posición: " i > "/dev/stderr"
        }
    }
    if(!plot_idx || !genus_idx) {
        print "ERROR: No se encontraron columnas Plot o genus" > "/dev/stderr"
        print "plot_idx=" plot_idx ", genus_idx=" genus_idx > "/dev/stderr"
        exit 1
    }
    next
}
{
    if(plot_idx && genus_idx && NF >= genus_idx) {
        plot_val = $plot_idx
        genus_val = $genus_idx
        gsub(/^"|"$/, "", plot_val)
        gsub(/^"|"$/, "", genus_val)
        gsub(/^[ \t]+|[ \t]+$/, "", plot_val)
        gsub(/^[ \t]+|[ \t]+$/, "", genus_val)
        
        # Validar que genus no contenga caracteres extraños (números, puntos, comas)
        if(plot_val != "" && genus_val != "" && genus_val != "NA" && genus_val !~ /[0-9,.]/) {
            otu_id = plot_val "_" genus_val
            print otu_id "," genus_val
        }
    }
}' $SAMPLES | sort -u > samples_with_otu_ids.csv

CREATED_OTUS=$(wc -l < samples_with_otu_ids.csv)
echo "OTU IDs únicos creados: $CREATED_OTUS"

if [ $CREATED_OTUS -eq 0 ]; then
    echo "ERROR: No se pudieron crear OTU IDs. Verifica las columnas Plot y genus"
    exit 1
fi

# 2. Identificar OTUs nuevos
echo -e "\n=== Paso 2: Identificando OTUs nuevos ==="

tail -n +2 $OTU_TABLE | cut -f1 | sed 's/"//g' | sed 's/^[ \t]*//;s/[ \t]*$//' | sort > existing_otus.txt

cut -d',' -f1 samples_with_otu_ids.csv | sort > new_otus_candidates.txt

comm -13 existing_otus.txt new_otus_candidates.txt > otus_to_add_only.txt

NEW_OTU_COUNT=$(wc -l < otus_to_add_only.txt)
echo "OTUs existentes: $(wc -l < existing_otus.txt)"
echo "OTUs únicos en Samples: $(wc -l < new_otus_candidates.txt)"
echo "OTUs a agregar: $NEW_OTU_COUNT"

if [ $NEW_OTU_COUNT -eq 0 ]; then
    echo "No hay OTUs nuevos. Archivos sincronizados."
    rm -f samples_with_otu_ids.csv existing_otus.txt new_otus_candidates.txt otus_to_add_only.txt
    exit 0
fi

# Crear archivo con OTUs a agregar y sus genus
grep -f otus_to_add_only.txt samples_with_otu_ids.csv > otus_to_add.csv

# 3. Actualizar OTU_table (OPTIMIZADO)
echo -e "\n=== Paso 3: Actualizando OTU_table ==="

NUM_SAMPLES=$(head -n 1 $OTU_TABLE | awk -F'\t' '{print NF-1}')
echo "Número de muestras: $NUM_SAMPLES"
echo "Agregando $NEW_OTU_COUNT OTUs..."

# Copiar archivo original
cp $OTU_TABLE $OTU_OUTPUT

# Crear string de ceros una sola vez
ZEROS=""
for ((i=1; i<=NUM_SAMPLES; i++)); do
    ZEROS="${ZEROS}\t0"
done

# Agregar todos los OTUs nuevos en un solo paso (MUCHO MÁS RÁPIDO)
cut -d',' -f1 otus_to_add.csv | while read -r otu_id; do
    echo -e "${otu_id}${ZEROS}"
done >> $OTU_OUTPUT

echo "OTU_table actualizado: $(tail -n +2 $OTU_OUTPUT | wc -l) OTUs totales"

# 4. Actualizar Tax_table (OPTIMIZADO)
echo -e "\n=== Paso 4: Actualizando Tax_table ==="

cp $TAX_TABLE $TAX_OUTPUT

# Crear todas las filas nuevas de una vez
awk -F',' '{
    otu_id = $1
    genus = $2
    # Crear fila con todas las columnas necesarias (ajusta según tu Tax_table)
    printf "%s,0,0,k__Fungi;p__NA;c__NA;o__NA;f__NA;g__%s;s__NA,Fungi,NA,NA,NA,NA,%s,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA\n", otu_id, genus, genus
}' otus_to_add.csv >> $TAX_OUTPUT

echo "Tax_table actualizado: $(tail -n +2 $TAX_OUTPUT | wc -l) taxa totales"

# 5. Resumen final
echo -e "\n=== Resumen de sincronización ==="
echo "Archivos actualizados:"
echo "  - $OTU_OUTPUT: $(tail -n +2 $OTU_OUTPUT | wc -l) OTUs"
echo "  - $TAX_OUTPUT: $(tail -n +2 $TAX_OUTPUT | wc -l) taxa"
echo ""
echo "OTUs agregados: $NEW_OTU_COUNT"
echo ""
echo "Primeros 10 OTUs agregados:"
head -10 otus_to_add_only.txt

# Limpiar temporales
rm -f samples_with_otu_ids.csv existing_otus.txt new_otus_candidates.txt otus_to_add_only.txt otus_to_add.csv

echo -e "\n=== Proceso completado ==="
echo "Verifica los archivos:"
echo "  tail -20 $OTU_OUTPUT | head -5"
echo "  tail -20 $TAX_OUTPUT | head -5"