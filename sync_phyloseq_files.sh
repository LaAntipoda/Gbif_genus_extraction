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

awk -F',' 'NR==1 {
    plot_idx=0; genus_idx=0
    for(i=1; i<=NF; i++) {
        col = $i
        gsub(/^[ \t]+|[ \t]+$/, "", col)
        gsub(/^"|"$/, "", col)
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
        
        if(plot_val != "" && genus_val != "" && genus_val != "NA" && genus_val !~ /[0-9,.]/) {
            otu_id = plot_val "_" genus_val
            print otu_id "," genus_val "," plot_val
        }
    }
}' $SAMPLES | sort -u > samples_with_otu_ids.csv

CREATED_OTUS=$(wc -l < samples_with_otu_ids.csv)
echo "OTU IDs únicos creados: $CREATED_OTUS"

if [ $CREATED_OTUS -eq 0 ]; then
    echo "ERROR: No se pudieron crear OTU IDs"
    exit 1
fi

echo "Primeros 5 OTUs creados:"
head -5 samples_with_otu_ids.csv

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

# Crear archivo con OTUs a agregar, sus genus y plots
grep -f otus_to_add_only.txt samples_with_otu_ids.csv > otus_to_add.csv

echo "Primeros 5 OTUs a agregar:"
head -5 otus_to_add.csv

# 3. Extraer todos los valores únicos de Plot de meta_metadata_2.csv
echo -e "\n=== Paso 3: Extrayendo sitios únicos de metadata ==="

awk -F',' 'NR==1 {
    for(i=1; i<=NF; i++) {
        col = $i
        gsub(/^[ \t]+|[ \t]+$/, "", col)
        gsub(/^"|"$/, "", col)
        if(col=="Plot") {
            plot_idx=i
            break
        }
    }
    next
}
{
    if(plot_idx) {
        plot_val = $plot_idx
        gsub(/^"|"$/, "", plot_val)
        gsub(/^[ \t]+|[ \t]+$/, "", plot_val)
        if(plot_val != "") print plot_val
    }
}' $SAMPLES | sort -u > all_plots.txt

NUM_PLOTS=$(wc -l < all_plots.txt)
echo "Sitios únicos encontrados en metadata: $NUM_PLOTS"

# 4. Crear OTU table actualizada con Python
echo -e "\n=== Paso 4: Creando OTU table actualizada ==="

cat > create_updated_otu_table.py << 'EOF'
import pandas as pd
import numpy as np
import sys

def create_updated_otu_table(original_otu, otus_to_add_file, all_plots_file, output_file):
    print("Leyendo OTU table original...")
    otu_original = pd.read_csv(original_otu, sep='\t', index_col=0)
    
    print(f"Dimensiones originales: {otu_original.shape}")
    print(f"Columnas existentes: {list(otu_original.columns[:10])}...")
    
    # Leer todos los plots (sitios) del metadata
    print("\nLeyendo sitios del metadata...")
    with open(all_plots_file, 'r') as f:
        all_plots = [line.strip() for line in f if line.strip()]
    
    print(f"Total de sitios en metadata: {len(all_plots)}")
    
    # Leer OTUs a agregar (formato: otu_id,genus,plot)
    print("\nLeyendo OTUs a agregar...")
    otus_data = []
    with open(otus_to_add_file, 'r') as f:
        for line in f:
            parts = line.strip().split(',')
            if len(parts) >= 3:
                otu_id = parts[0]
                genus = parts[1]
                plot = parts[2]
                otus_data.append({'otu': otu_id, 'genus': genus, 'plot': plot})
    
    print(f"OTUs a agregar: {len(otus_data)}")
    
    # Identificar columnas faltantes
    existing_cols = set(otu_original.columns)
    new_cols_needed = [plot for plot in all_plots if plot not in existing_cols]
    
    print(f"\nColumnas existentes en OTU table: {len(existing_cols)}")
    print(f"Columnas nuevas necesarias: {len(new_cols_needed)}")
    
    if new_cols_needed:
        print(f"Primeras 10 columnas nuevas: {new_cols_needed[:10]}")
    
    # OPTIMIZACIÓN: Agregar todas las columnas nuevas de una vez
    if new_cols_needed:
        new_cols_df = pd.DataFrame(0, index=otu_original.index, columns=new_cols_needed, dtype=np.int32)
        otu_original = pd.concat([otu_original, new_cols_df], axis=1)
    
    all_columns = list(otu_original.columns)
    print(f"\nTotal de columnas en nueva tabla: {len(all_columns)}")
    
    # Crear nuevas filas para OTUs nuevos
    print("\nCreando filas para OTUs nuevos...")
    
    # OPTIMIZACIÓN: Crear todas las filas nuevas de una vez usando NumPy
    n_new_otus = len(otus_data)
    n_cols = len(otu_original.columns)
    
    # Crear matriz de ceros
    new_data_matrix = np.zeros((n_new_otus, n_cols), dtype=np.int32)
    new_otu_names = []
    
    # Crear índice de columnas para acceso rápido
    col_to_idx = {col: idx for idx, col in enumerate(otu_original.columns)}
    
    matches_found = 0
    
    for i, otu_data in enumerate(otus_data):
        if (i + 1) % 1000 == 0:
            print(f"  Procesando OTU {i+1}/{n_new_otus}...")
        
        otu_id = otu_data['otu']
        plot = otu_data['plot']
        
        new_otu_names.append(otu_id)
        
        # Marcar presencia (1) en la columna del Plot correspondiente
        if plot in col_to_idx:
            new_data_matrix[i, col_to_idx[plot]] = 1
            matches_found += 1
    
    print(f"\n✓ OTUs con presencia asignada: {matches_found}/{n_new_otus}")
    
    # Crear DataFrame con las nuevas filas
    new_rows_df = pd.DataFrame(new_data_matrix, index=new_otu_names, columns=otu_original.columns)
    otu_updated = pd.concat([otu_original, new_rows_df])
    
    print(f"\nDimensiones finales: {otu_updated.shape}")
    
    # Guardar
    print(f"Guardando {output_file}...")
    otu_updated.to_csv(output_file, sep='\t')
    
    print("\n✓ OTU table actualizada exitosamente!")
    
    # Mostrar estadísticas
    print("\nEstadísticas finales:")
    print(f"  - OTUs totales: {otu_updated.shape[0]}")
    print(f"  - Sitios totales: {otu_updated.shape[1]}")
    print(f"  - OTUs agregados: {len(otus_data)}")
    print(f"  - Columnas agregadas: {len(new_cols_needed)}")

if __name__ == "__main__":
    create_updated_otu_table(
        "OTU_table_hipogeos.csv",
        "otus_to_add.csv",
        "all_plots.txt",
        "Otu_table_updated.csv"
    )
EOF

python3 create_updated_otu_table.py

if [ $? -ne 0 ]; then
    echo "ERROR: Falló la creación de OTU table"
    exit 1
fi

# 5. Actualizar Tax_table
echo -e "\n=== Paso 5: Actualizando Tax_table ==="

cp $TAX_TABLE $TAX_OUTPUT

awk -F',' '{
    otu_id = $1
    genus = $2
    printf "%s,0,0,k__Fungi;p__NA;c__NA;o__NA;f__NA;g__%s;s__NA,Fungi,NA,NA,NA,NA,%s,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA\n", otu_id, genus, genus
}' otus_to_add.csv >> $TAX_OUTPUT

echo "Tax_table actualizado: $(tail -n +2 $TAX_OUTPUT | wc -l) taxa totales"

# 6. Resumen final
echo -e "\n=== Resumen de sincronización ==="
echo "Archivos actualizados:"
echo "  - $OTU_OUTPUT: $(tail -n +2 $OTU_OUTPUT | wc -l) OTUs"
echo "  - $TAX_OUTPUT: $(tail -n +2 $TAX_OUTPUT | wc -l) taxa"
echo ""
echo "OTUs agregados: $NEW_OTU_COUNT"
echo "Columnas en OTU table: $(head -1 $OTU_OUTPUT | tr '\t' '\n' | wc -l)"

# Limpiar temporales
rm -f samples_with_otu_ids.csv existing_otus.txt new_otus_candidates.txt 
rm -f otus_to_add_only.txt otus_to_add.csv all_plots.txt create_updated_otu_table.py

echo -e "\n=== Proceso completado ==="
echo "Para verificar:"
echo "  tail -5 $OTU_OUTPUT | cut -f1-10 | column -t"
