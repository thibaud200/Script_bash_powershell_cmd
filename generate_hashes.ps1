param (
    [string]$Path = $(throw "Path is required"),
    [string]$OutputJson = $(throw "OutputJson is required")
)

$LogFile = "process.log"

# Nettoyer ou créer le fichier log
if (Test-Path $LogFile) {
    Clear-Content -Path $LogFile
} else {
    New-Item -ItemType File -Path $LogFile | Out-Null
}

function Log-Message {
    param ([string]$Message)
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "[$timestamp] $Message"
    Add-Content -Path $LogFile -Value $line
}

Log-Message "=== Début du scan sur $Path ==="

# Charger les fichiers déjà dans JSON
$existingPaths = @{}
if (Test-Path $OutputJson) {
    Log-Message "Chargement des hashes existants depuis $OutputJson ..."
    Get-Content $OutputJson | ForEach-Object {
        try {
            $obj = $_ | ConvertFrom-Json
            $existingPaths[$obj.Path] = $true
        } catch {
            Log-Message "Erreur JSON ligne: $_"
        }
    }
    Log-Message "Chargés $($existingPaths.Count) fichiers existants."
} else {
    Log-Message "Fichier JSON inexistant. Démarrage d'un nouveau fichier."
}

# Récupérer les fichiers
$files = Get-ChildItem -Path $Path -Recurse -File -ErrorAction SilentlyContinue
$total = $files.Count
$counter = 0

foreach ($file in $files) {
    $counter++
    Write-Progress -Activity "Hashing files" -Status "$counter / $total" -PercentComplete (($counter / $total) * 100)

    Log-Message "Parcouru : $($file.FullName)"

    if ($existingPaths.ContainsKey($file.FullName)) {
        Log-Message "Ignoré (déjà dans JSON) : $($file.FullName)"
        continue
    }

    try {
        $hash = Get-FileHash -Path $file.FullName -Algorithm SHA256
        $record = [PSCustomObject]@{
            Path = $file.FullName
            Hash = $hash.Hash
            Size = $file.Length
        }
        $jsonLine = $record | ConvertTo-Json -Depth 3 -Compress

        # Append dans JSON
        $jsonLine | Out-File -FilePath $OutputJson -Append -Encoding UTF8

        $existingPaths[$file.FullName] = $true
        Log-Message "Ajouté au JSON : $($file.FullName)"
    }
    catch {
        Log-Message "Erreur sur fichier : $($file.FullName) - $_"
    }
}

Log-Message "=== Scan terminé. JSON mis à jour dans : $OutputJson ==="
Write-Host "Scan terminé. Logs disponibles dans : $LogFile"
