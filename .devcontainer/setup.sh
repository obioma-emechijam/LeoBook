#!/usr/bin/env bash
set -euo pipefail

echo "=== LeoBook Codespace Auto-Setup (API 36) ==="

export DEBIAN_FRONTEND=noninteractive

# ---- 0. System dependencies ----
echo "[0/7] Installing system dependencies..."
sudo apt-get update -qq
sudo apt-get install -y -qq wget unzip curl git > /dev/null 2>&1 || true

# ---- 1. Python Dependencies ----
echo "[1/7] Installing Python dependencies..."
pip install --upgrade pip -q 2>/dev/null || true
[ -f requirements.txt ] && pip install -r requirements.txt -q || true
[ -f requirements-rl.txt ] && pip install -r requirements-rl.txt -q || true

# ---- 2. Playwright ----
echo "[2/7] Installing Playwright browsers..."
python -m playwright install-deps 2>/dev/null || true
python -m playwright install chromium 2>/dev/null || true

# ---- 3. Data Directories ----
echo "[3/7] Creating data directories..."
mkdir -p Data/Store/{models,Assets}
mkdir -p Data/Store/crests/{teams,leagues,flags}
mkdir -p Modules/Assets/{logos,crests}

# ---- 4. Flutter SDK ----
echo "[4/7] Installing Flutter SDK..."
FLUTTER_HOME="/home/vscode/flutter"
if [ ! -d "$FLUTTER_HOME" ]; then
    echo "  Cloning Flutter stable..."
    git clone https://github.com/flutter/flutter.git -b stable "$FLUTTER_HOME" --depth 1 || true
fi
# Ensure flutter is executable and precache
if [ -f "$FLUTTER_HOME/bin/flutter" ]; then
    echo "  Flutter found at $FLUTTER_HOME"
    "$FLUTTER_HOME/bin/flutter" --version || true
    "$FLUTTER_HOME/bin/flutter" precache --web 2>/dev/null || true
else
    echo "  WARNING: Flutter SDK not found after clone!"
fi

# ---- 5. Android SDK ----
echo "[5/7] Installing Android SDK..."
export ANDROID_HOME="/home/vscode/android-sdk"
mkdir -p "$ANDROID_HOME"

if [ ! -d "$ANDROID_HOME/cmdline-tools/latest" ]; then
    echo "  Downloading Android SDK command-line tools..."
    mkdir -p "$ANDROID_HOME/cmdline-tools"
    cd /tmp
    wget -q https://dl.google.com/android/repository/commandlinetools-linux-11076708_latest.zip -O cmdline-tools.zip 2>/dev/null || true
    if [ -f cmdline-tools.zip ]; then
        unzip -q -o cmdline-tools.zip -d "$ANDROID_HOME/cmdline-tools/" 2>/dev/null || true
        if [ -d "$ANDROID_HOME/cmdline-tools/cmdline-tools" ]; then
            mv "$ANDROID_HOME/cmdline-tools/cmdline-tools" "$ANDROID_HOME/cmdline-tools/latest" 2>/dev/null || true
        fi
        rm -f cmdline-tools.zip
    fi
    cd - > /dev/null
fi

# Accept Android licenses
mkdir -p "$ANDROID_HOME/licenses"
echo -e "\n24333f8a63b6825ea9c5514f83c2829b004d1fee" > "$ANDROID_HOME/licenses/android-sdk-license"
echo -e "\nd56f5187479451eabf01fb78af6dfcb131b33910" >> "$ANDROID_HOME/licenses/android-sdk-license"

# Install platform tools + build tools (needs sdkmanager on PATH)
SDKMANAGER="$ANDROID_HOME/cmdline-tools/latest/bin/sdkmanager"
if [ -f "$SDKMANAGER" ]; then
    echo "  Installing platforms and build-tools..."
    "$SDKMANAGER" "platform-tools" "platforms;android-36" "build-tools;36.0.0" > /dev/null 2>&1 || true
fi

# ---- 6. Flutter config ----
echo "[6/7] Configuring Flutter..."
if [ -f "$FLUTTER_HOME/bin/flutter" ]; then
    "$FLUTTER_HOME/bin/flutter" config --android-sdk "$ANDROID_HOME" 2>/dev/null || true
fi

# Flutter app dependencies
if [ -d "leobookapp" ]; then
    echo "  Running flutter pub get in leobookapp..."
    cd leobookapp
    "$FLUTTER_HOME/bin/flutter" pub get 2>/dev/null || true
    cd ..
fi

# ---- 7. VS Code settings ----
echo "[7/7] Configuring VS Code..."
mkdir -p .vscode
[ ! -f .vscode/settings.json ] && cat > .vscode/settings.json << 'EOF'
{
  "python.terminal.useEnvFile": true,
  "[python]": {
    "editor.defaultFormatter": "ms-python.python",
    "editor.formatOnSave": true
  }
}
EOF

echo ""
echo "============================================"
echo "  ✓ LeoBook Setup Complete!"
echo "============================================"
echo "  Flutter:     $FLUTTER_HOME"
echo "  Android SDK: $ANDROID_HOME"
echo "  API Level:   36"
echo ""
echo "  Verify: flutter doctor"
echo ""