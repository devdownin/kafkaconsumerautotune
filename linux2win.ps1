param(
    [string]$Dossier = (Get-Location).Path
)

Write-Host "Conversion des fichiers .java (LF -> CRLF + suppression BOM)" -ForegroundColor Cyan
Write-Host "===============================================================" -ForegroundColor Cyan
Write-Host "Dossier : $Dossier`n"

$encodingSansBOM = New-Object System.Text.UTF8Encoding $false  # UTF-8 sans BOM

$fichiers = Get-ChildItem -Path $Dossier -Filter "*.java" -Recurse
$compteur = 0

foreach ($f in $fichiers) {
    Write-Host "Traitement : $($f.FullName)"

    $content = [System.IO.File]::ReadAllText($f.FullName)
    
    # Supprimer le BOM s'il est présent
    $content = $content -replace "^\ufeff", ""
    
    # Convertir LF -> CRLF
    $content = $content -replace "(?<!`r)`n", "`r`n"
    
    [System.IO.File]::WriteAllText($f.FullName, $content, $encodingSansBOM)
    
    $compteur++
}

Write-Host "`n$compteur fichier(s) converti(s)." -ForegroundColor Green