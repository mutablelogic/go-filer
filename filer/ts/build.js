import esbuild from 'esbuild';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';
import { cpSync } from 'fs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const buildDir = resolve(__dirname, '../../build/ts');

// Build TypeScript → build/ts/dist/
await esbuild.build({
  entryPoints: ['src/index.ts'],
  bundle: true,
  outdir: `${buildDir}/dist`,
  format: 'esm',
  target: 'es2021',
  minify: true,
  sourcemap: true,
}).catch(() => process.exit(1));

// Copy source assets (index.html etc.) → build/ts/dist/
cpSync(resolve(__dirname, 'assets/dist'), `${buildDir}/dist`, { recursive: true });

// Copy embed.go → build/ts/embed.go
cpSync(resolve(__dirname, 'assets/embed.go'), `${buildDir}/embed.go`);

console.log(`Built → ${buildDir}`);
