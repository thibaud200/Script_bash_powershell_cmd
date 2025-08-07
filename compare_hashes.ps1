function Load-JsonLinesWithProgress($path) {
    $lines = Get-Content -Path $path
    $count = $lines.Count
    $list = @()
    for ($i=0; $i -lt $count; $i++) {
        try {
            $obj = $lines[$i] | ConvertFrom-Json
            $list += $obj
        } catch {
            Write-Warning "Ligne JSON invalide ignorée"
        }
        Write-Progress -Activity "Chargement JSON" -Status "Fichier $path" -PercentComplete (($i / $count) * 100)
    }
    Write-Progress -Activity "Chargement JSON" -Completed
    return $list
}

function Get-FileHashesWithProgress($path) {
    Write-Host "Scanning directory: $path"
    $files = Get-ChildItem -Path $path -Recurse -File
    $count = $files.Count
    $results = @()
    for ($i=0; $i -lt $count; $i++) {
        $file = $files[$i]
        try {
            $hash = Get-FileHash -Path $file.FullName -Algorithm SHA256
            $results += [PSCustomObject]@{
                Hash = $hash.Hash
                Path = $file.FullName
                Size = $file.Length
            }
        } catch {
            Write-Warning "Erreur hash fichier : $($file.FullName)"
        }
        Write-Progress -Activity "Calcul des hashes" -Status "Fichier $($file.Name)" -PercentComplete (($i / $count) * 100)
    }
    Write-Progress -Activity "Calcul des hashes" -Completed
    return $results
}

# Puis remplacer les appels Load-JsonLines et Get-FileHashes par
# Load-JsonLinesWithProgress et Get-FileHashesWithProgress respectivement dans le script principal
