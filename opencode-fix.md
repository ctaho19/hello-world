# Backup the bundled binary
sudo mv /opt/homebrew/Cellar/opencode/0.14.3/libexec/lib/node_modules/opencode-ai/node_modules/opencode-darwin-arm64/bin/opencode /opt/homebrew/Cellar/opencode/0.14.3/libexec/lib/node_modules/opencode-ai/node_modules/opencode-darwin-arm64/bin/opencode.real

# Create the shim
sudo tee /opt/homebrew/Cellar/opencode/0.14.3/libexec/lib/node_modules/opencode-ai/node_modules/opencode-darwin-arm64/bin/opencode > /dev/null << 'EOF'
#!/bin/bash

# Intercept the provider install command
if [[ "$1" == "add" && "$*" == *"@ai-sdk/openai-compatible"* ]]; then
  # Create the package in cache if not already there
  mkdir -p ~/.cache/opencode/node_modules/@ai-sdk/openai-compatible
  
  if [ ! -f ~/.cache/opencode/node_modules/@ai-sdk/openai-compatible/index.js ]; then
    cp /Users/uno201/opencode/opencode-offline-bundle/github-copilot-provider.bundle.cjs ~/.cache/opencode/node_modules/@ai-sdk/openai-compatible/index.js
    
    cat > ~/.cache/opencode/node_modules/@ai-sdk/openai-compatible/package.json << 'PKGJSON'
{
  "name": "@ai-sdk/openai-compatible",
  "version": "999.0.0",
  "main": "index.js",
  "type": "commonjs"
}
PKGJSON
  fi
  
  echo "Using local bundled provider (offline mode)" >&2
  exit 0
fi

# Pass everything else to the real binary
exec "$(dirname "$0")/opencode.real" "$@"
EOF

# Make it executable
sudo chmod +x /opt/homebrew/Cellar/opencode/0.14.3/libexec/lib/node_modules/opencode-ai/node_modules/opencode-darwin-arm64/bin/opencode
