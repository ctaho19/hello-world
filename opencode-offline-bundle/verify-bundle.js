// Quick test to verify the bundle loads
try {
  const provider = require('./github-copilot-provider.bundle.cjs');
  console.log('✅ Bundle loaded successfully');
  console.log('Exported keys:', Object.keys(provider));
  
  // Look for createXXX function
  const createFn = Object.keys(provider).find(k => k.startsWith('create'));
  if (createFn) {
    console.log(`✅ Found creator function: ${createFn}`);
  } else {
    console.log('⚠️  No create function found');
  }
} catch (err) {
  console.error('❌ Bundle failed to load:', err.message);
  process.exit(1);
}
