param (
    [Parameter(Mandatory)]
    [string]$DirectoryPath,
    [Parameter(Mandatory)]
    [string]$JsonHashFile,
    [string]$OutputReport = "duplicates_report.txt"
)

function Load-JsonLinesWithProgress($path) {
    Write-Host "Chargement du fichier JSON de hashes..." -ForegroundColor Cyan
    $lines = Get-Content -Path $path
    $count = $lines.Count
    $hashMap = @{}
    
    for ($i = 0; $i -lt $count; $i++) {
        try {
            $obj = $lines[$i] | ConvertFrom-Json
            if ($obj.Hash -and $obj.Path) {
                # Utiliser le hash comme clé pour une recherche rapide
                if (-not $hashMap.ContainsKey($obj.Hash)) {
                    $hashMap[$obj.Hash] = @()
                }
                $hashMap[$obj.Hash] += $obj
            }
        } catch {
            Write-Warning "Ligne JSON invalide ignorée (ligne $($i+1))"
        }
        
        if ($i % 1000 -eq 0) {
            Write-Progress -Activity "Chargement JSON" -Status "Ligne $i/$count" -PercentComplete (($i / $count) * 100)
        }
    }
    
    Write-Progress -Activity "Chargement JSON" -Completed
    Write-Host "JSON chargé: $($hashMap.Keys.Count) hashes uniques trouvés" -ForegroundColor Green
    return $hashMap
}

function Get-FileHashesWithProgress($path) {
    Write-Host "Scan du répertoire: $path" -ForegroundColor Cyan
    
    try {
        $files = Get-ChildItem -Path $path -Recurse -File -ErrorAction Stop
    } catch {
        Write-Error "Erreur d'accès au répertoire: $($_.Exception.Message)"
        return @{}
    }
    
    $count = $files.Count
    $hashMap = @{}
    
    Write-Host "Calcul des hashes pour $count fichiers..." -ForegroundColor Yellow
    
    for ($i = 0; $i -lt $count; $i++) {
        $file = $files[$i]
        try {
            # Utiliser -LiteralPath pour gérer les caractères spéciaux
            $hash = Get-FileHash -LiteralPath $file.FullName -Algorithm SHA256 -ErrorAction Stop
            
            $fileObj = [PSCustomObject]@{
                Hash = $hash.Hash
                Path = $file.FullName
                Size = $file.Length
                Name = $file.Name
            }
            
            # Utiliser le hash comme clé
            if (-not $hashMap.ContainsKey($hash.Hash)) {
                $hashMap[$hash.Hash] = @()
            }
            $hashMap[$hash.Hash] += $fileObj
            
        } catch [System.UnauthorizedAccessException] {
            Write-Warning "Accès refusé: $($file.FullName)"
        } catch [System.IO.IOException] {
            Write-Warning "Erreur E/S: $($file.FullName)"
        } catch {
            Write-Warning "Erreur hash fichier: $($file.FullName) - $($_.Exception.Message)"
        }
        
        if ($i % 50 -eq 0 -or $i -eq ($count - 1)) {
            Write-Progress -Activity "Calcul des hashes" -Status "Fichier $($file.Name)" -PercentComplete (($i / $count) * 100)
        }
    }
    
    Write-Progress -Activity "Calcul des hashes" -Completed
    Write-Host "Hashes calculés: $($hashMap.Keys.Count) hashes uniques dans le répertoire" -ForegroundColor Green
    return $hashMap
}

function Find-Duplicates($directoryHashes, $jsonHashes) {
    Write-Host "Recherche des doublons..." -ForegroundColor Cyan
    
    $duplicates = @()
    $totalHashes = $directoryHashes.Keys.Count
    $processed = 0
    
    foreach ($hash in $directoryHashes.Keys) {
        $processed++
        
        if ($jsonHashes.ContainsKey($hash)) {
            # Doublon trouvé !
            $dirFiles = $directoryHashes[$hash]
            $jsonFiles = $jsonHashes[$hash]
            
            foreach ($dirFile in $dirFiles) {
                foreach ($jsonFile in $jsonFiles) {
                    $duplicate = [PSCustomObject]@{
                        Hash = $hash
                        DirectoryFile = $dirFile.Path
                        DirectorySize = $dirFile.Size
                        JsonFile = $jsonFile.Path
                        JsonSize = $jsonFile.Size
                        SizesMatch = $dirFile.Size -eq $jsonFile.Size
                    }
                    $duplicates += $duplicate
                }
            }
        }
        
        if ($processed % 100 -eq 0) {
            Write-Progress -Activity "Recherche doublons" -Status "Hash $processed/$totalHashes" -PercentComplete (($processed / $totalHashes) * 100)
        }
    }
    
    Write-Progress -Activity "Recherche doublons" -Completed
    return $duplicates
}

function Generate-Report($duplicates, $outputPath) {
    Write-Host "Génération du rapport..." -ForegroundColor Cyan
    
    $report = @()
    $report += "RAPPORT DE DETECTION DE DOUBLONS"
    $report += "================================="
    $report += "Date: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
    $report += "Nombre de doublons trouvés: $($duplicates.Count)"
    $report += ""
    
    if ($duplicates.Count -eq 0) {
        $report += "Aucun doublon détecté !"
    } else {
        # Grouper par hash pour un affichage plus clair
        $groupedDuplicates = $duplicates | Group-Object Hash
        
        foreach ($group in $groupedDuplicates) {
            $report += "HASH: $($group.Name)"
            $report += "Nombre de correspondances: $($group.Count)"
            
            foreach ($dup in $group.Group) {
                $report += "  REPERTOIRE: $($dup.DirectoryFile) ($([math]::Round($dup.DirectorySize/1MB, 2)) MB)"
                $report += "  JSON      : $($dup.JsonFile) ($([math]::Round($dup.JsonSize/1MB, 2)) MB)"
                
                if (-not $dup.SizesMatch) {
                    $report += "  ⚠️  ATTENTION: Tailles différentes!"
                }
                $report += ""
            }
            $report += "---"
        }
        
        # Statistiques
        $totalSizeMB = ($duplicates | Measure-Object DirectorySize -Sum).Sum / 1MB
        $report += ""
        $report += "STATISTIQUES:"
        $report += "Espace total des doublons: $([math]::Round($totalSizeMB, 2)) MB"
        $report += "Espace récupérable: $([math]::Round($totalSizeMB, 2)) MB"
    }
    
    # Sauvegarder le rapport
    $report | Out-File -FilePath $outputPath -Encoding UTF8
    
    # Affichage console
    Write-Host ""
    Write-Host "=== RÉSULTATS ===" -ForegroundColor Magenta
    Write-Host "Doublons trouvés: $($duplicates.Count)" -ForegroundColor $(if ($duplicates.Count -gt 0) { "Red" } else { "Green" })
    
    if ($duplicates.Count -gt 0) {
        $totalSizeMB = ($duplicates | Measure-Object DirectorySize -Sum).Sum / 1MB
        Write-Host "Espace récupérable: $([math]::Round($totalSizeMB, 2)) MB" -ForegroundColor Yellow
        Write-Host "Rapport détaillé: $outputPath" -ForegroundColor Cyan
        
        # Afficher les premiers doublons
        Write-Host ""
        Write-Host "Premiers doublons trouvés:" -ForegroundColor Yellow
        $duplicates | Select-Object -First 5 | ForEach-Object {
            Write-Host "  • $($_.DirectoryFile)" -ForegroundColor White
            Write-Host "    = $($_.JsonFile)" -ForegroundColor Gray
        }
        
        if ($duplicates.Count -gt 5) {
            Write-Host "  ... et $($duplicates.Count - 5) autres (voir rapport complet)" -ForegroundColor Gray
        }
    }
}

# SCRIPT PRINCIPAL
Write-Host "DETECTEUR DE DOUBLONS" -ForegroundColor Magenta
Write-Host "Répertoire: $DirectoryPath" -ForegroundColor White
Write-Host "Fichier JSON: $JsonHashFile" -ForegroundColor White
Write-Host ""

# Vérifications
if (-not (Test-Path $DirectoryPath)) {
    Write-Error "Répertoire introuvable: $DirectoryPath"
    exit 1
}

if (-not (Test-Path $JsonHashFile)) {
    Write-Error "Fichier JSON introuvable: $JsonHashFile"
    exit 1
}

try {
    # 1. Charger les hashes du JSON
    $jsonHashes = Load-JsonLinesWithProgress $JsonHashFile
    
    # 2. Calculer les hashes du répertoire
    $directoryHashes = Get-FileHashesWithProgress $DirectoryPath
    
    # 3. Trouver les doublons
    $duplicates = Find-Duplicates $directoryHashes $jsonHashes
    
    # 4. Générer le rapport
    Generate-Report $duplicates $OutputReport
    
    Write-Host ""
    Write-Host "TERMINÉ!" -ForegroundColor Green
    
} catch {
    Write-Error "Erreur durant l'exécution: $($_.Exception.Message)"
    exit 1
}
