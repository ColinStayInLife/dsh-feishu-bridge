# -*- coding: utf-8 -*-
# run_hidden.ps1 — 计划任务 DSH-Feishu-Bridge / DSH-Web 的隐藏启动器
# 由 schtasks 调用：powershell -File run_hidden.ps1 -Component Bridge|Web
param(
    [ValidateSet("Bridge", "Web")]
    [string]$Component = "Bridge"
)

$ErrorActionPreference = "Continue"
$env:PYTHONIOENCODING = "utf-8"
$OutputEncoding = [System.Text.UTF8Encoding]::new($false)
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$PSDefaultParameterValues['Out-File:Encoding'] = 'utf8'
$launcherMutex = [System.Threading.Mutex]::new($false, "Local\DSH-Feishu-$Component")
try { $ownsLauncher = $launcherMutex.WaitOne(0) }
catch [System.Threading.AbandonedMutexException] { $ownsLauncher = $true }
if (-not $ownsLauncher) { $launcherMutex.Dispose(); exit 0 }
$base = Split-Path -Parent $MyInvocation.MyCommand.Path
$env:DSH_FEISHU_BRIDGE_DIR = $base
$logDir = Join-Path $base "logs"
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Force -Path $logDir | Out-Null }
$log = Join-Path $logDir ("{0}_stdout.log" -f $Component.ToLower())
Set-Location $base

function Write-Log($msg) {
    $line = "{0} [run_hidden] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $msg
    Add-Content -Path $log -Value $line -Encoding UTF8
}

# ---- Python 解析：优先系统 Python310（venv 已损坏，不再依赖） ----
$pyCandidates = @(
    "$env:LOCALAPPDATA\Programs\Python\Python310\python.exe",
    "$env:LOCALAPPDATA\Programs\Python\Python311\python.exe",
    "$env:LOCALAPPDATA\Programs\Python\Python312\python.exe",
    "python.exe"
)
$py = $null
foreach ($c in $pyCandidates) {
    if ($c -eq "python.exe") { $py = "python.exe"; break }
    if (Test-Path $c) { $py = $c; break }
}

if ($Component -eq "Bridge") {
    $script = Join-Path $base "bridge_env.py"
    if (-not (Test-Path $script)) { $script = Join-Path $base "bridge.py" }
} else {
    $py = $null
    # DSH 会扫描工作目录的 .env；桥的 DSH_API_BASE 属于它禁止的启动键。
    Set-Location (Join-Path $env:USERPROFILE ".dsh")
    # Web 组件：交给 dsh 的绝对路径（安装时写入 web_entry.txt）
    $entryFile = Join-Path $base "web_entry.txt"
    if (Test-Path $entryFile) {
        $script = (Get-Content $entryFile -Raw).Trim()
    } else {
        $dshCommand = Get-Command dsh.cmd -ErrorAction SilentlyContinue
        $script = if ($dshCommand) { $dshCommand.Source } else { "dsh" }
    }
}

Write-Log "start component=$Component py=$py script=$script"

# ---- 守护循环：进程退出后 15 秒重启；连续快速失败则退避 ----
$failures = 0
while ($true) {
    $started = Get-Date
    try {
        if ($py) {
            & $py $script *>> $log
        } else {
            & $script web --patch (Join-Path $base "web.patch.yml") --host 127.0.0.1 --port 3080 --no-open *>> $log
        }
        $code = $LASTEXITCODE
    } catch {
        Write-Log ("exception: {0}" -f $_.Exception.Message)
        $code = -1
    }
    $ran = ((Get-Date) - $started).TotalSeconds

    if ($ran -lt 60) { $failures++ } else { $failures = 0 }
    $wait = if ($failures -ge 3) { [Math]::Min(300, 30 * $failures) } else { 15 }

    Write-Log ("exited code={0} after {1:N0}s, restart in {2}s (failures={3})" -f $code, $ran, $wait, $failures)
    Start-Sleep -Seconds $wait
}
