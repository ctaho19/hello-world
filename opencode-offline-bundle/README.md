# GitHub Copilot Provider Bundle for OpenCode

## Files
- `github-copilot-provider.bundle.cjs` - Bundled provider for offline use
- `verify-bundle.js` - Test script to verify bundle works
- `README.md` - This file

## Verification (run on internet-connected machine)
```bash
node verify-bundle.js
```

## Copy to Corporate Machine
1. Copy the entire `opencode-offline-bundle` folder to your work device
2. Note the absolute path where you copy it

## Usage on Corporate Machine

### Method 1: Environment Variable
```bash
export OPENCODE_DISABLE_AUTO_INSTALL=1
export OPENCODE_PROVIDER_PATH=/absolute/path/to/github-copilot-provider.bundle.cjs
export NODE_EXTRA_CA_CERTS=/path/to/corporate-root-certs.pem

opencode chat
```

### Method 2: Config File
Edit `~/.opencode/config.json`:
```json
{
  "provider": {
    "github-copilot": {
      "options": {
        "modulePath": "/absolute/path/to/github-copilot-provider.bundle.cjs"
      }
    }
  }
}
```

Then run:
```bash
export OPENCODE_DISABLE_AUTO_INSTALL=1
export NODE_EXTRA_CA_CERTS=/path/to/corporate-root-certs.pem
opencode chat
```

## Troubleshooting
If you get module errors, you may need to externalize fewer dependencies.
See the bundling script for options.
