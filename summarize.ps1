[CmdletBinding()]
param([string]$Prefix = '*')

$ErrorActionPreference = 'Stop'
$resultDir = Join-Path $PSScriptRoot 'results'
$rows = [System.Collections.Generic.List[object]]::new()

function Metric {
    param($Report, [string]$Name, [string]$Value)
    $entry = $Report.metrics.PSObject.Properties[$Name]
    if ($null -eq $entry) { return $null }
    return $entry.Value.values.$Value
}

Get-ChildItem -LiteralPath $resultDir -Filter "$Prefix*.json" | Sort-Object Name | ForEach-Object {
    $report = Get-Content -LiteralPath $_.FullName -Raw | ConvertFrom-Json
    if ($null -eq $report.config -or $null -eq $report.config.phases) { return }
    foreach ($phase in $report.config.phases) {
        if ($phase.kind -ne 'measurement') { continue }
        $duration = $phase.duration_ms / 1000.0
        foreach ($op in @('write', 'timeline', 'attributes', 'tags')) {
            $filter = "phase:$($phase.name),op:$op"
            $count = Metric $report "operations{$filter}" 'count'
            if ($null -eq $count) { continue }
            $rows.Add([pscustomobject][ordered]@{
                run = $report.config.run_id
                target_rps = $phase.target
                operation = $op
                requests = $count
                completed_rps = [Math]::Round($count / $duration, 2)
                http_p95_ms = [Math]::Round((Metric $report "http_req_duration{$filter}" 'p(95)'), 2)
                http_p99_ms = [Math]::Round((Metric $report "http_req_duration{$filter}" 'p(99)'), 2)
                db_p95_ms = [Math]::Round((Metric $report "db_ms{$filter}" 'p(95)'), 2)
                error_rate = Metric $report "request_errors{phase:$($phase.name)}" 'rate'
                documents_per_s = if ($op -eq 'write') { [Math]::Round((Metric $report "write_documents{phase:$($phase.name)}" 'count') / $duration, 2) } else { $null }
                dropped_in_run = Metric $report 'dropped_iterations' 'count'
            })
        }
    }
}

if ($rows.Count -eq 0) { throw 'No k6 summaries found for this prefix.' }
$csv = Join-Path $resultDir 'comparison.csv'
$rows | Export-Csv -LiteralPath $csv -NoTypeInformation -Encoding utf8
$rows | Format-Table -AutoSize
Write-Host "CSV: $csv"
