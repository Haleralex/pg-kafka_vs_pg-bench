[CmdletBinding()]
param(
    [ValidateSet('pg_gin_path_ops', 'pg_gin_ops', 'pg_targeted', 'mongo_targeted')]
    [string[]]$Profiles = @('pg_gin_path_ops', 'pg_targeted', 'mongo_targeted'),
    [ValidateRange(1000, 2000000)][int]$SeedCount = 200000,
    [string]$RateSteps = '100,300,600',
    [string]$StepDuration = '30s',
    [string]$WarmupDuration = '20s',
    [string]$TransitionDuration = '10s',
    [ValidateRange(0, 100)][int]$WritePercent = 70,
    [ValidateRange(1, 1000)][int]$BatchSize = 10,
    [ValidateRange(1, 65535)][int]$Port = 18088,
    [ValidateRange(1, 10)][int]$Repetitions = 1,
    [switch]$SkipBuild,
    [switch]$KeepRunning
)

$ErrorActionPreference = 'Stop'
$root = $PSScriptRoot
$resultDir = Join-Path $root 'results'
$baseURL = "http://127.0.0.1:$Port"
$stamp = [DateTime]::UtcNow.ToString('yyyyMMdd-HHmmss')
$oldProfile = $env:BENCH_PROFILE
$oldPort = $env:BENCH_PORT
$env:BENCH_PORT = "$Port"
$allRuns = [System.Collections.Generic.List[object]]::new()
$baseline = $null
$loadProcess = $null

function Invoke-Compose {
    param([string[]]$DockerArgs)
    & docker compose -f (Join-Path $root 'compose.yaml') @DockerArgs
    if ($LASTEXITCODE -ne 0) { throw "docker compose failed ($LASTEXITCODE): $DockerArgs" }
}

function Save-JSON {
    param($Value, [string]$Name)
    $Value | ConvertTo-Json -Depth 100 | Set-Content -LiteralPath (Join-Path $resultDir $Name) -Encoding utf8
}

function Wait-API {
    $deadline = [DateTime]::UtcNow.AddSeconds(90)
    do {
        try { return Invoke-RestMethod "$baseURL/health" -TimeoutSec 3 }
        catch { Start-Sleep -Seconds 1 }
    } while ([DateTime]::UtcNow -lt $deadline)
    throw 'The benchmark API did not become healthy.'
}

try {
    New-Item -ItemType Directory -Path $resultDir -Force | Out-Null
    & docker info --format '{{json .}}' | Set-Content (Join-Path $resultDir "$stamp-docker-info.json") -Encoding utf8
    if ($LASTEXITCODE -ne 0) { throw 'Docker Desktop must be running with Linux containers.' }
    Invoke-Compose -DockerArgs @('config', '--quiet')
    if (-not $SkipBuild) { Invoke-Compose -DockerArgs @('build', 'api') }
    Invoke-Compose -DockerArgs @('pull', 'postgres', 'mongo', 'k6')
    & docker image inspect postgres:17 mongo:8.0 grafana/k6:1.3.0 gotraining/docbench-api:local |
        Set-Content (Join-Path $resultDir "$stamp-images.json") -Encoding utf8

    for ($repetition = 1; $repetition -le $Repetitions; $repetition++) {
        # Reverse alternate runs to expose order and filesystem-cache effects.
        $order = @($Profiles)
        if ($repetition % 2 -eq 0) { [Array]::Reverse($order) }
        foreach ($profile in $order) {
            $runId = "$stamp-r$repetition-$profile"
            $env:BENCH_PROFILE = $profile
            Write-Host "Starting $runId"
            Invoke-Compose -DockerArgs @('stop', 'api', 'postgres', 'mongo')
            $database = if ($profile.StartsWith('pg_')) { 'postgres' } else { 'mongo' }
            Invoke-Compose -DockerArgs @('up', '-d', '--wait', $database)
            Invoke-Compose -DockerArgs @('up', '-d', '--no-deps', '--force-recreate', 'api')
            $health = Wait-API
            Save-JSON -Value $health -Name "$runId-health.json"

            Write-Host "Seeding $SeedCount events and validating query results..."
            $seed = Invoke-RestMethod "$baseURL/admin/seed" -Method Post -ContentType 'application/json' `
                -Body (@{count = $SeedCount; batch_size = 1000} | ConvertTo-Json -Compress) -TimeoutSec 1200
            Save-JSON -Value $seed -Name "$runId-seed.json"
            Save-JSON -Value (Invoke-RestMethod "$baseURL/stats" -TimeoutSec 60) -Name "$runId-before.json"

            $baseTime = [DateTimeOffset]::Parse('2026-10-01T00:00:00Z').ToUnixTimeMilliseconds()
            $signatures = [ordered]@{}
            $plans = [ordered]@{}
            foreach ($kind in @('timeline', 'attributes', 'tags')) {
                foreach ($tenant in @(1, 17, 51)) {
                    $query = "kind=$kind&tenant=$tenant&from_ms=$baseTime&to_ms=$($baseTime + $SeedCount + 1)&service=svc-00&level=error&tag=tag-00&limit=50"
                    $response = Invoke-RestMethod "$baseURL/read?$query" -TimeoutSec 60
                    $signatures["$kind-$tenant"] = @($response.events | ForEach-Object { $_.id })
                    if ($tenant -eq 1) { $plans[$kind] = Invoke-RestMethod "$baseURL/explain?$query" -TimeoutSec 60 }
                }
            }
            $signatureJSON = $signatures | ConvertTo-Json -Depth 10 -Compress
            if ($null -eq $baseline) { $baseline = $signatureJSON }
            elseif ($baseline -cne $signatureJSON) { throw "Cross-profile query result mismatch: $runId" }
            Save-JSON -Value $signatures -Name "$runId-correctness.json"
            Save-JSON -Value $plans -Name "$runId-plans.json"

            $arguments = @('compose', '-f', 'compose.yaml', 'run', '--rm', '--no-deps',
                '-e', "RUN_ID=$runId", '-e', "SEED_COUNT=$SeedCount", '-e', "RATE_STEPS=$RateSteps",
                '-e', "STEP_DURATION=$StepDuration", '-e', "WARMUP_DURATION=$WarmupDuration",
                '-e', "TRANSITION_DURATION=$TransitionDuration", '-e', "WRITE_PERCENT=$WritePercent",
                '-e', "BATCH_SIZE=$BatchSize", '-e', "BENCH_PROFILE=$profile", 'k6')
            $startArgs = @{
                FilePath = (Get-Command docker).Source; ArgumentList = $arguments
                WorkingDirectory = $root; PassThru = $true
                RedirectStandardOutput = (Join-Path $resultDir "$runId-k6.log")
                RedirectStandardError = (Join-Path $resultDir "$runId-k6-stderr.log")
            }
            # -WindowStyle exists only on Windows; pwsh on Linux rejects it.
            if ($PSVersionTable.PSEdition -eq 'Desktop' -or $IsWindows) { $startArgs.WindowStyle = 'Hidden' }
            $loadProcess = Start-Process @startArgs
            $resourceSamples = [System.Collections.Generic.List[object]]::new()
            while (-not $loadProcess.HasExited) {
                $containerIDs = @(& docker ps -q --filter 'label=com.docker.compose.project=gotraining-docbench')
                if ($containerIDs.Count -gt 0) {
                    $stats = @(& docker stats --no-stream --format '{{json .}}' @containerIDs)
                    foreach ($line in $stats) {
                        if ($line.StartsWith('{')) {
                            $resourceSamples.Add([ordered]@{ at = [DateTime]::UtcNow.ToString('o'); stats = ($line | ConvertFrom-Json) })
                        }
                    }
                }
                Start-Sleep -Seconds 2
                $loadProcess.Refresh()
            }
            $loadProcess.WaitForExit()
            $exitCode = $loadProcess.ExitCode
            $loadProcess = $null
            Save-JSON -Value @($resourceSamples.ToArray()) -Name "$runId-resources.json"
            Get-Content -LiteralPath (Join-Path $resultDir "$runId-k6.log") -Tail 35
            # k6 code 99 means thresholds failed; the measurements remain useful.
            if ($exitCode -notin @(0, 99)) { throw "k6 failed with exit code $exitCode; see $runId-k6-stderr.log" }
            Save-JSON -Value (Invoke-RestMethod "$baseURL/stats" -TimeoutSec 120) -Name "$runId-after.json"
            Save-JSON -Value (Invoke-RestMethod "$baseURL/admin/maintain" -Method Post -TimeoutSec 120) -Name "$runId-maintenance.json"
            Save-JSON -Value (Invoke-RestMethod "$baseURL/stats" -TimeoutSec 120) -Name "$runId-maintained.json"
            Invoke-Compose -DockerArgs @('logs', '--no-color', 'api', $database) |
                Set-Content (Join-Path $resultDir "$runId-containers.log") -Encoding utf8
            $allRuns.Add([ordered]@{id = $runId; profile = $profile; repetition = $repetition; k6_exit = $exitCode; correctness = 'equal'})
            Save-JSON -Value @($allRuns.ToArray()) -Name "$stamp-runs.json"
        }
    }
    Write-Host "Results: $resultDir"
}
finally {
    if ($null -ne $loadProcess -and -not $loadProcess.HasExited) {
        & docker compose -f (Join-Path $root 'compose.yaml') --profile load stop k6
        Stop-Process -Id $loadProcess.Id -ErrorAction SilentlyContinue
    }
    if (-not $KeepRunning) { & docker compose -f (Join-Path $root 'compose.yaml') stop api postgres mongo }
    $env:BENCH_PROFILE = $oldProfile
    $env:BENCH_PORT = $oldPort
}
