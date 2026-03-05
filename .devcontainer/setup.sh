#!/usr/bin/env bash
# =============================================================
# LeoBook — Codespace Auto-Setup (runs on container creation)
# Installs Python, Playwright, Flutter, Android SDK in one shot.
# Idempotent — safe to re-run.
# =============================================================
set -euo pipefail

echo "=== LeoBook Codespace Auto-Setup ==="
echo "  Environment: $(uname -s) $(uname -m)"
echo "  Python:      $(python --version 2>&1)"
echo ""

# ---- 1. Core Python Dependencies ----
echo "[1/7] Installing core Python dependencies..."
pip install --upgrade pip -q
pip install -r requirements.txt -q

# ---- 2. RL / PyTorch (CPU-only) ----
echo "[2/7] Installing PyTorch CPU + RL dependencies..."
pip install -r requirements-rl.txt -q

# ---- 3. Playwright Browsers ----
echo "[3/7] Installing Playwright browsers..."
python -m playwright install-deps 2>/dev/null || true
python -m playwright install chromium

# ---- 4. Create Data Directories ----
echo "[4/7] Creating data directories..."
mkdir -p Data/Store/models
mkdir -p Data/Store/Assets
mkdir -p Data/Store/crests/teams
mkdir -p Data/Store/crests/leagues
mkdir -p Data/Store/crests/flags
mkdir -p Modules/Assets/logos
mkdir -p Modules/Assets/crests

# ---- 5. Flutter SDK ----
echo "[5/7] Installing Flutter SDK..."
if [ ! -d "$HOME/flutter" ]; then
    git clone https://github.com/flutter/flutter.git -b stable "$HOME/flutter" --depth 1
else
    echo "  Flutter already installed, skipping clone."
fi
export PATH="$PATH:$HOME/flutter/bin"
grep -q 'flutter/bin' ~/.bashrc 2>/dev/null || echo 'export PATH="$PATH:$HOME/flutter/bin"' >> ~/.bashrc
flutter precache --android 2>/dev/null || true
flutter --version

# ---- 6. Android SDK (for APK builds) ----
echo "[6/7] Installing Android SDK..."
ANDROID_HOME="$HOME/android-sdk"
export ANDROID_HOME
export PATH="$PATH:$ANDROID_HOME/cmdline-tools/latest/bin:$ANDROID_HOME/platform-tools"

if [ ! -d "$ANDROID_HOME/cmdline-tools/latest" ]; then
    mkdir -p "$ANDROID_HOME/cmdline-tools"
    cd "$ANDROID_HOME/cmdline-tools"
    wget -q https://dl.google.com/android/repository/commandlinetools-linux-11076708_latest.zip -O tools.zip
    unzip -q tools.zip && mv cmdline-tools latest && rm tools.zip
    cd /workspaces/LeoBook
else
    echo "  Android SDK already installed, skipping."
fi

grep -q 'ANDROID_HOME' ~/.bashrc 2>/dev/null || {
    echo "export ANDROID_HOME=\"$ANDROID_HOME\"" >> ~/.bashrc
    echo 'export PATH="$PATH:$ANDROID_HOME/cmdline-tools/latest/bin:$ANDROID_HOME/platform-tools"' >> ~/.bashrc
}

yes | sdkmanager --licenses 2>/dev/null || true
sdkmanager "platform-tools" "platforms;android-34" "build-tools;34.0.0" 2>/dev/null || true
flutter config --android-sdk "$ANDROID_HOME"

# ---- 7. Flutter App Dependencies ----
echo "[7/7] Getting Flutter app dependencies..."
if [ -d "leobookapp" ]; then
    cd leobookapp && flutter pub get && cd ..
fi

# ---- VS Code Settings (env file support) ----
VSCODE_SETTINGS=".vscode/settings.json"
mkdir -p .vscode
if [ ! -f "$VSCODE_SETTINGS" ]; then
    echo '{"python.terminal.useEnvFile": true}' > "$VSCODE_SETTINGS"
fi

# ---- Done ----
echo ""
echo "============================================"
echo "  LeoBook Setup Complete!"
echo "============================================"
echo "  Python:    python Leo.py --help"
echo "  Sync:      python Leo.py --sync"
echo "  RL:        python Leo.py --train-rl"
echo "  Flutter:   cd leobookapp && flutter build apk --release"
echo "  Analyze:   cd leobookapp && flutter analyze"
echo ""
echo "  Data export/import:"
echo "    zip:   cd Data/Store && zip -r ../leobook-export.zip leobook.db crests/"
echo "    unzip: cd Data/Store && unzip ../leobook-export.zip"
echo "============================================"
