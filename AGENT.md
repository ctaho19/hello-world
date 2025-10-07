#!/bin/bash
# Intercept OpenCode's provider install and use our local bundle instead

# Check if this is the OpenCode provider install command
if [[ "$1" == "add" && "$2" == "--force" && "$3" == "--extract" && "$6" == "@ai-sdk/openai-compatible@latest" ]]; then
  # Ensure our bundled provider is in place
  mkdir -p ~/.cache/opencode/node_modules/@ai-sdk/openai-compatible
  
  # Copy bundle if not already there
  if [ ! -f ~/.cache/opencode/node_modules/@ai-sdk/openai-compatible/index.js ]; then
    cp ~/opencode-offline-bundle/github-copilot-provider.bundle.cjs ~/.cache/opencode/node_modules/@ai-sdk/openai-compatible/index.js
    
    # Create package.json
    cat > ~/.cache/opencode/node_modules/@ai-sdk/openai-compatible/package.json << 'PKGJSON'
{
  "name": "@ai-sdk/openai-compatible",
  "version": "999.0.0",
  "main": "index.js",
  "type": "commonjs"
}
PKGJSON
  fi
  
  echo "Using local bundled provider (offline mode)"
  exit 0
fi

# For all other commands, use the real bun
exec /Users/uno201/.bun/bin/bun "$@"
