# 1. Install Verdaccio (2 mins)
npm install -g verdaccio

# 2. Start it in background (1 min)
verdaccio &
# It runs on http://localhost:4873

# 3. Create a proper package from your bundle (5 mins)
mkdir -p ~/verdaccio-package
cd ~/verdaccio-package
cat > package.json << 'EOF'
{
  "name": "@ai-sdk/openai-compatible",
  "version": "1.0.0-local",
  "main": "index.js",
  "type": "commonjs"
}
EOF
cp /Users/uno201/opencode/opencode-offline-bundle/github-copilot-provider.bundle.cjs index.js

# 4. Publish to Verdaccio (2 mins)
npm set registry http://localhost:4873
npm adduser --registry http://localhost:4873
# Username: test, Password: test, Email: test@test.com
npm publish --registry http://localhost:4873

# 5. Configure Bun to use Verdaccio (5 mins)
echo '[install.scopes]' >> ~/.bunfig.toml
echo '"@ai-sdk" = { registry = "http://localhost:4873" }' >> ~/.bunfig.toml

# 6. Test
OPENCODE_DISABLE_DEFAULT_PLUGINS=1 opencode chat
