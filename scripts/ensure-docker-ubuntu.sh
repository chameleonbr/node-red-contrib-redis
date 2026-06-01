#!/usr/bin/env bash
set -euo pipefail

has_working_docker() {
  command -v docker >/dev/null 2>&1 &&
    docker info >/dev/null 2>&1 &&
    docker compose version >/dev/null 2>&1
}

has_working_sudo_docker() {
  command -v docker >/dev/null 2>&1 &&
    command -v sudo >/dev/null 2>&1 &&
    sudo -n docker info >/dev/null 2>&1 &&
    sudo -n docker compose version >/dev/null 2>&1
}

if has_working_docker || has_working_sudo_docker; then
  exit 0
fi

if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  if command -v systemctl >/dev/null 2>&1; then
    sudo systemctl start docker >/dev/null 2>&1 || true
  fi
  if has_working_docker || has_working_sudo_docker; then
    exit 0
  fi
  echo "Docker is installed, but the current user cannot run 'docker info'." >&2
  echo "Fix Docker daemon access, or make passwordless sudo available for Docker, before running npm test." >&2
  exit 1
fi

if [[ ! -r /etc/os-release ]]; then
  echo "Docker is required, and automatic installation is only supported on Ubuntu." >&2
  exit 1
fi

. /etc/os-release

if [[ "${ID:-}" != "ubuntu" ]]; then
  echo "Docker is required, and automatic installation is only supported on Ubuntu." >&2
  exit 1
fi

case "${VERSION_ID:-}" in
  22.04|24.04|26.04) ;;
  *)
    echo "Docker is required. Automatic installation supports Ubuntu 22.04, 24.04, and 26.04 only." >&2
    exit 1
    ;;
esac

if ! command -v sudo >/dev/null 2>&1 && [[ "${EUID}" -ne 0 ]]; then
  echo "Docker is required, but sudo is not available for installation." >&2
  exit 1
fi

SUDO=()
if [[ "${EUID}" -ne 0 ]]; then
  SUDO=(sudo)
fi

tmp_key="$(mktemp)"
trap 'rm -f "$tmp_key"' EXIT

"${SUDO[@]}" apt-get update
"${SUDO[@]}" apt-get install -y ca-certificates curl
"${SUDO[@]}" install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o "$tmp_key"
"${SUDO[@]}" install -m 0644 "$tmp_key" /etc/apt/keyrings/docker.asc
"${SUDO[@]}" chmod a+r /etc/apt/keyrings/docker.asc

arch="$(dpkg --print-architecture)"
codename="${VERSION_CODENAME}"
repo="deb [arch=${arch} signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu ${codename} stable"
echo "$repo" | "${SUDO[@]}" tee /etc/apt/sources.list.d/docker.list >/dev/null

"${SUDO[@]}" apt-get update
"${SUDO[@]}" apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

if command -v systemctl >/dev/null 2>&1; then
  "${SUDO[@]}" systemctl enable --now docker
fi

if ! has_working_docker && ! has_working_sudo_docker; then
  echo "Docker was installed, but the current user cannot run 'docker info' yet." >&2
  echo "Add the user to the docker group or run from a shell with passwordless sudo Docker access." >&2
  exit 1
fi
