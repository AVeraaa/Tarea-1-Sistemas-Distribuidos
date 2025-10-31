import google.generativeai as genai
import os
import sys

print("Iniciando script para listar modelos...")

try:
    gemini_api_key = os.environ['GEMINI_API_KEY']
    genai.configure(api_key=gemini_api_key)
    print(
        f"API Key encontrada y configurada (termina en '...{gemini_api_key[-4:]}').")
except KeyError:
    print("\nError: La variable de entorno GEMINI_API_KEY no está configurada en el contenedor.")
    print("Asegúrate de que esté en el .env y que el docker-compose.yml la esté pasando.")
    sys.exit(1)
except Exception as e:
    print(f"\nError inesperado al configurar la API: {e}")
    sys.exit(1)


print("\nBuscando modelos disponibles que soporten 'generateContent':")
print("---------------------------------------------------------")

try:
    count = 0
    for model in genai.list_models():
        if 'generateContent' in model.supported_generation_methods:
            print(f"  -> {model.name}")
            count += 1

    if count == 0:
        print("\n¡Error! No se encontró ningún modelo que soporte 'generateContent'.")
        print("Esto puede ser un problema con los permisos de tu API Key.")

except Exception as e:
    print(f"\nError al intentar listar los modelos: {e}")

print("---------------------------------------------------------")
print("Script finalizado. Copia uno de los nombres de la lista (ej. 'models/gemini-1.5-flash-latest')")
print("y pégalo en 'llm-module/main.py' reemplazando a 'gemini-pro'.")
