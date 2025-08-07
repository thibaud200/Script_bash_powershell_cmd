param (
    [Parameter(Mandatory)]
    [string]$Path,
    [Parameter(Mandatory)]
    [string]$OutputJson
)

# Log
$LogFile = "process.log"
if (Test-Path $LogFile) { Clear-Content $LogFile } else { New-Item -Path $LogFile -ItemType File | Out-Null }

function Write-Log {
    param([string]$Message)
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Add-Content -Path $LogFile -Value "[$timestamp] $Message"
}

Write-Log "=== PRODUCER/CONSUMER ==="

# Loading files that are already indexes - NDJSON SUPPORT
$existingPaths = @{}
if (Test-Path $OutputJson) {
    Write-Log "Loading exiting hashes from $OutputJson (format NDJSON)..."
    try {
        $lineCount = 0
        Get-Content $OutputJson -ErrorAction Stop | ForEach-Object {
            $lineCount++
            if ($_.Trim() -ne "") {
                try {
                    $obj = $_ | ConvertFrom-Json
                    if ($obj.Path) {
                        $existingPaths[$obj.Path] = $true
                    }
                } catch {
                    Write-Log "JSON Error on line $lineCount : $_"
                }
            }
        }
        Write-Log "Loaded $($existingPaths.Count) existing files from $lineCount lines."
    } catch {
        Write-Log "ERROR while loading the JSON: $($_.Exception.Message)"
    }
} else {
    # Créer un fichier vide (pas d'array, juste vide)
    New-Item -Path $OutputJson -ItemType File -Force | Out-Null
    Write-Log "JSON files non existant. Created empty."
}

# Récupération des fichiers
Write-Host "Scan files from $Path ..." -ForegroundColor Cyan
try {
    $allFiles = Get-ChildItem -Path $Path -Recurse -File -ErrorAction Stop
} catch {
    Write-Host "ERROR while scanning: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

$files = $allFiles | Where-Object { -not $existingPaths.ContainsKey($_.FullName) }
$totalFiles = $files.Count
$skippedFiles = $allFiles.Count - $totalFiles

Write-Host "Files founds : $($allFiles.Count)" -ForegroundColor White
Write-Host "Already precessed : $skippedFiles" -ForegroundColor Gray
Write-Host "To precess : $totalFiles" -ForegroundColor Green

if ($totalFiles -eq 0) {
    Write-Host "No new files to precess." -ForegroundColor Green
    exit 0
}

# QUEUE THREAD-SAFE pour les résultats
$resultQueue = [System.Collections.Concurrent.ConcurrentQueue[object]]::new()
$completedThreads = 0
$totalProcessed = 0
$stopWriter = $false

# Groupes pour les threads producteurs
$threadCount = [Math]::Min(4, $totalFiles)
$groupSize = [Math]::Ceiling($totalFiles / $threadCount)
$groups = @()

for ($i = 0; $i -lt $threadCount; $i++) {
    $startIndex = $i * $groupSize
    if ($startIndex -lt $totalFiles) {
        $endIndex = [Math]::Min($startIndex + $groupSize - 1, $totalFiles - 1)
        $groupFiles = $files[$startIndex..$endIndex]
        if ($groupFiles.Count -gt 0) {
            $groups += ,$groupFiles
        }
    }
}

$threadCount = $groups.Count
Write-Log "Threads producteurs : $threadCount"

# THREAD CONSUMER/WRITER - FORMAT NDJSON
$writerScriptBlock = {
    param($queue, $jsonFile, $logFile)
    
    $savedCount = 0
    $batchSize = 20
    $batch = @()
    
    Write-Host "WRITER - Started (format NDJSON)!" -ForegroundColor Yellow
    Add-Content -Path $logFile -Value "[$(Get-Date -Format 'HH:mm:ss')] WRITER Started (NDJSON)"
    
    while ($true) {
        $foundData = $false
        
        # read elements from queue
        $result = $null
        while ($queue.TryDequeue([ref]$result)) {
            $batch += $result
            $foundData = $true
            
            Write-Host "WRITER - Element add to batch (taille: $($batch.Count))" -ForegroundColor Gray
            
            # If batch is full , save
            if ($batch.Count -ge $batchSize) {
                try {
                    Write-Host "WRITER - Save NDJSON of $($batch.Count) elements..." -ForegroundColor Yellow
                    
                    # SAVE TO NDJSON (line by line)
                    foreach ($item in $batch) {
                        $jsonLine = $item | ConvertTo-Json -Depth 3 -Compress
                        Add-Content -Path $jsonFile -Value $jsonLine -Encoding UTF8
                    }
                    
                    $savedCount += $batch.Count
                    $timestamp = Get-Date -Format "HH:mm:ss"
                    Write-Host "[$timestamp] WRITER - ✅ SAUVEGARDÉ $($batch.Count) lignes NDJSON (Total: $savedCount)" -ForegroundColor Green
                    
                    Add-Content -Path $logFile -Value "[$timestamp] WRITER sauvegardé NDJSON: $($batch.Count) (Total: $savedCount)"
                    
                    # Reset du batch
                    $batch = @()
                    
                } catch {
                    Write-Host "WRITER - ❌ ERREUR sauvegarde NDJSON: $($_.Exception.Message)" -ForegroundColor Red
                    Add-Content -Path $logFile -Value "[$(Get-Date -Format 'HH:mm:ss')] ERREUR WRITER: $($_.Exception.Message)"
                }
            }
        }
        
        # Pause si pas de données
        if (-not $foundData) {
            Start-Sleep -Milliseconds 200
        }
    }
}

# THREAD PRODUCER (hash files)
$producerScriptBlock = {
    param($files, $threadId, $queue)

    $counter = 0
    $successCount = 0
    $startTime = Get-Date

    Write-Host "Thread $threadId - DÉMARRÉ avec $($files.Count) fichiers" -ForegroundColor Magenta

    foreach ($file in $files) {
        $counter++

        try {
            # Test d'accès avec -LiteralPath
            if (-not (Test-Path -LiteralPath $file.FullName)) {
                Write-Host "Thread $threadId - Fichier introuvable: $($file.Name)" -ForegroundColor Red
                continue
            }

            # Hash avec LiteralPath pour les caractères spéciaux
            $hashStart = Get-Date
            $hash = Get-FileHash -LiteralPath $file.FullName -Algorithm SHA256 -ErrorAction Stop
            $hashTime = ((Get-Date) - $hashStart).TotalSeconds
            
            $obj = [PSCustomObject]@{
                Path = $file.FullName
                Hash = $hash.Hash
                Size = $file.Length
                ProcessedDate = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
                ThreadId = $threadId
            }

            # AJOUTER À LA QUEUE (thread-safe)
            $queue.Enqueue($obj)
            $successCount++
            
            # Affichage tous les 10 fichiers
            if ($counter % 10 -eq 0) {
                $elapsed = [math]::Round(((Get-Date) - $startTime).TotalMinutes, 1)
                $percent = [math]::Round(($counter / $files.Count) * 100, 1)
                $hashPreview = $hash.Hash.Substring(0,8)
                $fileSizeMB = [math]::Round($file.Length / 1MB, 1)
                
                Write-Host "Thread $threadId - $counter/$($files.Count) ($percent%) - $elapsed min - $($file.Name) [$hashPreview...] ($fileSizeMB MB) [Hash: $([math]::Round($hashTime,1))s]" -ForegroundColor Green
            }

        } catch [System.UnauthorizedAccessException] {
            Write-Host "Thread $threadId - ACCÈS REFUSÉ: $($file.Name)" -ForegroundColor Red
        } catch [System.IO.IOException] {
            Write-Host "Thread $threadId - ERREUR E/S: $($file.Name)" -ForegroundColor Red
        } catch {
            Write-Host "Thread $threadId - ERREUR: $($file.Name) - $($_.Exception.Message)" -ForegroundColor Red
        }
    }

    $totalTime = [math]::Round(((Get-Date) - $startTime).TotalMinutes, 1)
    Write-Host "Thread $threadId - TERMINÉ - $successCount/$($files.Count) fichiers hashés en $totalTime min" -ForegroundColor Cyan
    return $successCount
}

# Création du RunspacePool
Write-Host "DÉMARRAGE ARCHITECTURE PRODUCTEUR/CONSOMMATEUR..." -ForegroundColor Magenta
$runspacePool = [runspacefactory]::CreateRunspacePool(1, $threadCount + 1)  # +1 pour le writer
$runspacePool.Open()

$jobs = @()

# LANCER LE THREAD WRITER - VERSION SIMPLIFIÉE
Write-Host "Démarrage du thread WRITER..." -ForegroundColor Yellow
$writerPS = [PowerShell]::Create()
$writerPS.RunspacePool = $runspacePool
$writerPS.AddScript($writerScriptBlock).AddArgument($resultQueue).AddArgument($OutputJson).AddArgument($LogFile) | Out-Null
$writerAsync = $writerPS.BeginInvoke()

# LANCER LES THREADS PRODUCTEURS
for ($t = 0; $t -lt $threadCount; $t++) {
    $groupFiles = $groups[$t]
    $threadId = $t + 1

    Write-Host "Thread producteur $threadId - $($groupFiles.Count) fichiers assignés" -ForegroundColor Magenta

    $ps = [PowerShell]::Create()
    $ps.RunspacePool = $runspacePool
    $ps.AddScript($producerScriptBlock).AddArgument($groupFiles).AddArgument($threadId).AddArgument($resultQueue) | Out-Null

    $asyncResult = $ps.BeginInvoke()
    $jobs += [PSCustomObject]@{
        PowerShellInstance = $ps
        AsyncResult        = $asyncResult
        ThreadId           = $threadId
    }
}

# Suivi des threads producteurs
Write-Host "TRAITEMENT EN COURS..." -ForegroundColor Cyan

do {
    Start-Sleep -Seconds 3
    $runningJobs = $jobs | Where-Object { -not $_.AsyncResult.IsCompleted }
    $completedJobs = $threadCount - $runningJobs.Count
    $queueSize = $resultQueue.Count
    
    Write-Host "ÉTAT: $completedJobs/$threadCount producteurs terminés - Queue: $queueSize éléments en attente" -ForegroundColor Yellow
    
} while ($runningJobs.Count -gt 0)

Write-Host "TOUS LES PRODUCTEURS TERMINÉS!" -ForegroundColor Green

# Attendre que la queue soit vide - VERSION SIMPLIFIÉE
Write-Host "Attente que le WRITER termine..." -ForegroundColor Yellow
do {
    Start-Sleep -Seconds 1
    $queueSize = $resultQueue.Count
    if ($queueSize -gt 0) {
        Write-Host "Queue: $queueSize éléments restants" -ForegroundColor Gray
    }
} while ($queueSize -gt 0)

# Attendre encore un peu pour que le writer traite tout
Start-Sleep -Seconds 3
Write-Host "Arrêt du writer..." -ForegroundColor Yellow

# Arrêter le writer en fermant son PowerShell (il va sortir de sa boucle)
$writerPS.Stop()

# Récupération des résultats
$totalSuccess = 0
foreach ($job in $jobs) {
    try {
        $threadSuccess = $job.PowerShellInstance.EndInvoke($job.AsyncResult)
        $totalSuccess += $threadSuccess
        Write-Host "Thread producteur $($job.ThreadId) - $threadSuccess fichiers hashés" -ForegroundColor Green
    } catch {
        Write-Host "Erreur thread $($job.ThreadId): $($_.Exception.Message)" -ForegroundColor Red
    }
    $job.PowerShellInstance.Dispose()
}

# Résultat du writer
try {
    $writerResult = $writerPS.EndInvoke($writerAsync)
    Write-Host "Thread WRITER - $writerResult files saved" -ForegroundColor Green
} catch {
    Write-Host "Writer error: $($_.Exception.Message)" -ForegroundColor Red
}
$writerPS.Dispose()

$runspacePool.Close()
$runspacePool.Dispose()

Write-Host "TRAITEMENT TERMINÉ!" -ForegroundColor Green
Write-Host "hashed files: $totalSuccess" -ForegroundColor Green
Write-Host "JSON Files: $OutputJson" -ForegroundColor Cyan
Write-Host "Logs: $LogFile" -ForegroundColor Gray

Write-Log "=== producer/consumer finished - $totalSuccess files ==="