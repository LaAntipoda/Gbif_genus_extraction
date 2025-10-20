#Script para extraer observaciones de Gbif
#Yo y Claude Sonnet contra el mundo 

#paquetes
library(httr)
library(jsonlite)
library(tidyverse)

#URL de gbif
BASE_URL <- "https://api.gbif.org/v1/occurrence/search"

#Cargar lista de géneros
generos <- read.delim("archivo.txt", header = FALSE) %>%
  pull(1) %>%
  trimws()
#verificar que este completa
cat("Total de géneros:", length(generos), "\n")

# funcion de descarga 
descargar_genero_api <- function(genero, max_registros = 1000) { #aqui puedes editar la cantidad de registros maxima que quieres
  tryCatch({
    todos_registros <- list()
    offset <- 0
    batch_num <- 1
    
    while (TRUE) {
      # Parámetros de la API
      params <- list(
        q = genero,
        limit = 300,
        offset = offset
      )
      
      # Hacer request
      response <- GET(BASE_URL, query = params)
      
      if (status_code(response) != 200) {
        cat(genero, "- ERROR HTTP:", status_code(response), "\n")
        break
      }
      
      # Parsear JSON
      data <- fromJSON(content(response, as = "text"), simplifyDataFrame = TRUE)
      
      # Si no hay resultados, parar
      if (is.null(data$results) || nrow(data$results) == 0) {
        break
      }
      
      # Extraer y filtrar datos
      batch <- data$results %>%
        as_tibble() %>%
        filter(kingdom == "Fungi") %>%
        filter(tolower(trimws(genus)) == tolower(trimws(genero))) %>%
        select(
          species = scientificName,
          genus = genus,
          family = family,
          key = key,
          decimalLatitude = decimalLatitude,
          decimalLongitude = decimalLongitude
        ) %>%
        filter(!is.na(decimalLatitude), !is.na(decimalLongitude))
      
      if (nrow(batch) > 0) {
        todos_registros[[batch_num]] <- batch
        batch_num <- batch_num + 1
        
        # Contar total actual
        total_actual <- sum(sapply(todos_registros, nrow))
        cat("  ", genero, "batch", batch_num - 1, "-", nrow(batch), "registros (total:", total_actual, ")\n")
        
        # Si ya tenemos suficientes registros, parar
        if (total_actual >= max_registros) {
          break
        }
      }
      
      # Si obtuvimos menos de 300 registros, hemos llegado al final
      if (nrow(data$results) < 300) {
        break
      }
      
      offset <- offset + 300
      Sys.sleep(0.2)
    }
    
    if (length(todos_registros) > 0) {
      resultado_final <- bind_rows(todos_registros)
      cat(genero, "✓ COMPLETO:", nrow(resultado_final), "registros\n")
      return(resultado_final)
    } else {
      cat(genero, "✗ Sin registros\n")
      return(NULL)
    }
    
  }, error = function(e) {
    cat(genero, "- ERROR:", e$message, "\n")
    return(NULL)
  })
}

# Descargar los registros observados 
#dado que utilice una lista muy extensa, puedes darle control + c para pausar y volver a empezar desde donde se quedo
# Archivo de progreso
archivo_progreso <- "hongos_gbif_progreso.csv"
archivo_generos_hecho <- "generos_completados.txt"

# Verificar si hay progreso previo
if (file.exists(archivo_progreso)) {
  resultado <- read.csv(archivo_progreso, stringsAsFactors = FALSE)
  generos_hecho <- readLines(archivo_generos_hecho)
  generos_pendientes <- setdiff(generos, generos_hecho)
  cat("  -", nrow(resultado), "registros ya descargados\n")
  cat("  -", length(generos_hecho), "géneros completados\n")
  cat("  -", length(generos_pendientes), "géneros pendientes\n\n")
} else {
  cat("Iniciando descarga desde cero...\n\n")
  resultado <- data.frame()
  generos_hecho <- character()
  generos_pendientes <- generos
}

# Descargar géneros pendientes
for (i in seq_along(generos_pendientes)) {
  g <- generos_pendientes[i]
  cat("[", i, "/", length(generos_pendientes), "]", g, "\n")
  
  dat <- descargar_genero_api(g, max_registros = 500)
  
  if (!is.null(dat)) {
    resultado <- bind_rows(resultado, dat)
  }
  
  # Guardar progreso después de cada género
  write.csv(resultado, archivo_progreso, row.names = FALSE, na = "")
  generos_hecho <- c(generos_hecho, g)
  writeLines(generos_hecho, archivo_generos_hecho)
  
  Sys.sleep(0.5)
}

# Archivo final
cat("Total de registros:", nrow(resultado), "\n")

if (nrow(resultado) > 0) {
  cat("Géneros únicos:", n_distinct(resultado$genus), "\n")
  cat("Familias únicas:", n_distinct(resultado$family), "\n\n")
  
  cat("Primeros registros:\n")
  print(head(resultado, 10))
  
  # Guardar archivo final
  write.csv(resultado, "Hipogeos/Generos/hongos_gbif.csv", row.names = FALSE, na = "")
  cat("\n✓ Datos guardados en 'hongos_gbif.csv'\n")
  
  # Limpiar archivos de progreso
  file.remove(archivo_progreso)
  file.remove(archivo_generos_hecho)
  cat("✓ Archivos de progreso eliminados\n")
} else {
  cat("No se encontraron registros\n")
}