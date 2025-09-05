from mergeCSV import UnificarCsvCarpeta

input_path = r"C:\Users\Usuario\Desktop\2024-Tr\UnifacionCSVLinkedin\Linkedin\SubirFormLeadsLinkedin"
output_path = r"C:\Users\Usuario\Desktop\2024-Tr\UnifacionCSVLinkedin\Linkedin\FormLeadsLinkedin"
backup_path = r"C:\Users\Usuario\Desktop\2024-Tr\UnifacionCSVLinkedin\Linkedin\FormLeadsLinkedinBK"

salida, backup = UnificarCsvCarpeta(
    carpeta_entrada=input_path,
    archivo_salida=output_path,
    carpeta_backup=backup_path,
)

print("RESULTADO FINAL:")
print("Archivo generado:", salida)
print("Backup creado:", backup)
