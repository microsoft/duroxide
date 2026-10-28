# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

[CmdletBinding()]
param(
    [string] $TargetDirectory = 'target\lifecycle-production',
    [string] $EvidenceDirectory = 'target\lifecycle-production\evidence'
)

$ErrorActionPreference = 'Stop'
Push-Location (Split-Path -Parent $PSScriptRoot)
try {
    $root = (Get-Location).ProviderPath
    $target = [IO.Path]::GetFullPath($TargetDirectory, $root)
    $evidence = [IO.Path]::GetFullPath($EvidenceDirectory, $root)
    foreach ($path in @($target, $evidence)) {
        if (-not $path.StartsWith("$root\", [StringComparison]::OrdinalIgnoreCase)) {
            throw 'Production test outputs must remain within the core repository'
        }
    }
    New-Item -ItemType Directory -Force $target, $evidence | Out-Null
    $buildLog = Join-Path $evidence 'production-build.jsonl'
    $buildErrors = Join-Path $evidence 'production-build.stderr'
    & cargo build --quiet --locked --offline -j 4 -p duroxide --no-default-features --features sqlite --lib `
        --target-dir $target --message-format=json > $buildLog 2> $buildErrors
    if ($LASTEXITCODE -ne 0) {
        Get-Content $buildErrors
        throw "Production library build failed ($LASTEXITCODE)"
    }
    $artifacts = @(Get-Content $buildLog | ConvertFrom-Json | Where-Object reason -eq 'compiler-artifact')
    $core = @($artifacts | Where-Object { $_.target.name -eq 'duroxide' -and $_.target.kind -contains 'lib' })
    if ($core.Count -ne 1 -or $core[0].profile.test -or
        @($core[0].features | Where-Object { $_ -in @('test-hooks', 'test', 'provider-test', 'replay-version-test') }).Count -ne 0) {
        throw 'Expected exactly one hook-free, non-test core library artifact'
    }
    $recorderBuildLog = Join-Path $evidence 'production-recorder-build.jsonl'
    $recorderBuildErrors = Join-Path $evidence 'production-recorder-build.stderr'
    $rootLock = Join-Path $root 'Cargo.lock'
    $registryPattern = '(?m)^name = "([^"]+)"\r?\nversion = "([^"]+)"\r?\nsource = "([^"]+)"\r?\nchecksum = "([^"]+)"'
    $rootPackages = [regex]::Matches((Get-Content -Raw $rootLock), $registryPattern)
    $recorderPackage = @($rootPackages | Where-Object { $_.Groups[1].Value -eq 'metrics-util' })
    if ($recorderPackage.Count -ne 1) {
        throw 'Expected one locked metrics-util version for the test recorder'
    }
    $fixture = Join-Path $target 'recorder-fixture'
    New-Item -ItemType Directory -Force $fixture | Out-Null
    $manifest = Join-Path $fixture 'Cargo.toml'
    $fixtureLock = Join-Path $fixture 'Cargo.lock'
    $fixtureSource = Join-Path $root 'tests\common\retirement_metrics.rs' | ConvertTo-Json -Compress
    @"
[package]
name = "duroxide-production-recorder-fixture"
version = "0.0.0"
edition = "2024"
publish = false

[workspace]

[lib]
path = $fixtureSource

[dependencies]
metrics-util = "=$($recorderPackage[0].Groups[2].Value)"
"@ | Set-Content $manifest
    Copy-Item $rootLock $fixtureLock -Force
    $rustVersion = & rustc -vV
    if ($LASTEXITCODE -ne 0) {
        throw 'Could not determine the native Rust target'
    }
    $rustHost = @($rustVersion | Select-String '^host: (.+)$' | ForEach-Object { $_.Matches[0].Groups[1].Value })
    if ($rustHost.Count -ne 1) {
        throw 'Expected one native Rust target'
    }
    & cargo metadata --offline --manifest-path $manifest --format-version 1 --filter-platform $rustHost[0] `
        > (Join-Path $evidence 'production-recorder-resolution.json') 2> $recorderBuildErrors
    if ($LASTEXITCODE -ne 0) {
        Get-Content $recorderBuildErrors
        throw "Test recorder fixture resolution failed ($LASTEXITCODE)"
    }
    # Bootstrap only the generated fixture's lock, then require its registry graph
    # to be an exact subset of the unchanged repository lock before building.
    $knownPackages = @($rootPackages | ForEach-Object {
        ($_.Groups | Select-Object -Skip 1 | ForEach-Object Value) -join '|'
    })
    foreach ($package in [regex]::Matches((Get-Content -Raw $fixtureLock), $registryPattern)) {
        $key = ($package.Groups | Select-Object -Skip 1 | ForEach-Object Value) -join '|'
        if ($key -notin $knownPackages) {
            throw "Test recorder fixture introduced a registry dependency outside Cargo.lock: $key"
        }
    }
    & cargo build --quiet --locked --offline -j 4 --manifest-path $manifest --lib `
        --target-dir $target --message-format=json > $recorderBuildLog 2> $recorderBuildErrors
    if ($LASTEXITCODE -ne 0) {
        Get-Content $recorderBuildErrors
        throw "Test recorder build failed ($LASTEXITCODE)"
    }
    $recorderArtifacts = @(Get-Content $recorderBuildLog | ConvertFrom-Json | Where-Object reason -eq 'compiler-artifact')
    if (@($recorderArtifacts | Where-Object { $_.target.name -eq 'duroxide' }).Count -ne 0) {
        throw 'Building the test recorder must not rebuild the production core'
    }
    $externs = @()
    $libraries = @{}
    foreach ($name in @('duroxide', 'tokio', 'metrics', 'metrics_util')) {
        $source = if ($name -eq 'metrics_util') { $recorderArtifacts } else { $artifacts }
        $artifact = @($source | Where-Object { $_.target.name -eq $name -and $_.target.kind -contains 'lib' })
        if ($name -eq 'tokio') {
            $artifact = @($artifact | Where-Object { $_.features -contains 'full' })
        }
        $library = @($artifact.filenames | Where-Object { $_.EndsWith('.rlib') })
        if ($library.Count -ne 1) {
            throw "Expected one current $name rlib, found $($library.Count)"
        }
        $libraries[$name] = $library[0]
        $externs += @('--extern', "$name=$($library[0])")
    }
    $recorderFacade = @($recorderArtifacts | Where-Object { $_.target.name -eq 'metrics' })
    $recorderFacadeLibrary = @($recorderFacade.filenames | Where-Object { $_.EndsWith('.rlib') })
    if ($recorderFacadeLibrary.Count -ne 1 -or $recorderFacadeLibrary[0] -ne $libraries.metrics) {
        throw 'Production core and test recorder must use the identical metrics facade artifact'
    }
    # Cargo's dev-dependency enables test-hooks. Link the ordinary public-surface
    # tests to the proven production rlib and build the locked recorder fixture
    # separately, without selecting core's dev-dependencies.
    $suites = @(
        @{ name = 'lifecycle_production_tests'; prefix = 'production'; executable = 'lifecycle-production-tests.exe' },
        @{ name = 'foreign_retirement_metrics_tests'; prefix = 'production-metrics'; executable = 'foreign-retirement-metrics-tests.exe' }
    )
    $results = @()
    foreach ($suite in $suites) {
        $executable = Join-Path $target $suite.executable
        $harnessLog = Join-Path $evidence "$($suite.prefix)-harness-build.log"
        & rustc --edition 2024 --test "tests\$($suite.name).rs" --crate-name $suite.name `
            -L "dependency=$(Join-Path $target 'debug\deps')" @externs -o $executable `
            > $harnessLog 2>&1
        if ($LASTEXITCODE -ne 0) {
            Get-Content $harnessLog
            throw "Production $($suite.name) harness compilation failed ($LASTEXITCODE)"
        }
        $testLog = Join-Path $evidence "$($suite.prefix)-tests.log"
        & $executable --test-threads=1 --nocapture > $testLog 2>&1
        $testExit = $LASTEXITCODE
        $text = Get-Content -Raw $testLog
        $summary = [regex]::Match($text, 'test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;')
        if (-not $summary.Success) {
            Get-Content $testLog
            throw "Production $($suite.name) output has no completed test summary (possible abort)"
        }
        $passed = [int]$summary.Groups[1].Value
        $failed = [int]$summary.Groups[2].Value
        $skipped = [int]$summary.Groups[3].Value
        $results += [pscustomobject][ordered]@{
            name = $suite.name
            executable = $executable
            executableSha256 = (Get-FileHash $executable -Algorithm SHA256).Hash
            exitCode = $testExit
            selected = $passed + $failed + $skipped
            passed = $passed
            failed = $failed
            skipped = $skipped
            aborted = $false
            evidence = $testLog
        }
        Write-Output "$($suite.name): $($summary.Value)"
    }
    $failedSuites = @($results | Where-Object { $_.exitCode -ne 0 -or $_.passed -eq 0 -or $_.failed -ne 0 -or $_.skipped -ne 0 })
    [ordered]@{
        coreFeatures = $core[0].features
        coreTestProfile = $core[0].profile.test
        coreArtifact = $libraries.duroxide
        coreSha256 = (Get-FileHash $libraries.duroxide -Algorithm SHA256).Hash
        metricsFacadeArtifact = $libraries.metrics
        metricsFacadeSha256 = (Get-FileHash $libraries.metrics -Algorithm SHA256).Hash
        recorderArtifact = $libraries.metrics_util
        recorderSha256 = (Get-FileHash $libraries.metrics_util -Algorithm SHA256).Hash
        recorderManifest = $manifest
        recorderLock = $fixtureLock
        recorderLockSha256 = (Get-FileHash $fixtureLock -Algorithm SHA256).Hash
        suites = $results
        exitCode = if ($failedSuites.Count -eq 0) { 0 } else { 1 }
        selected = ($results | Measure-Object -Property selected -Sum).Sum
        passed = ($results | Measure-Object -Property passed -Sum).Sum
        failed = ($results | Measure-Object -Property failed -Sum).Sum
        skipped = ($results | Measure-Object -Property skipped -Sum).Sum
        aborted = $false
    } | ConvertTo-Json -Depth 6 | Set-Content (Join-Path $evidence 'production-provenance.json')
    if ($failedSuites.Count -ne 0) {
        foreach ($suite in $failedSuites) {
            Get-Content $suite.evidence
        }
        throw 'Production lifecycle execution failed or skipped required cases'
    }
} finally {
    Pop-Location
}
