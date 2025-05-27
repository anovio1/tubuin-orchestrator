# Start Docker Desktop (if not already running)
Start-Process "C:\Program Files\Docker\Docker\Docker Desktop.exe"

# Wait for Docker engine to be ready
Write-Host "Launching Prefect Server, needs Docker"
Write-Host "Launch Docker: Waiting for Docker to be ready..."
while ($true) {
    try {
        docker info | Out-Null
        break
    } catch {
        Start-Sleep -Seconds 2
    }
}
Write-Host "Good: Docker is running."

# Change into the prefect-server directory
Set-Location -Path ".\prefect-server"

# Prompt to start Prefect server
$input = Read-Host "NOTE* read below prefect config set... Start Prefect Server (docker compose up)? [Y/N]"
if ($input -eq "Y") {
    docker compose up
}
