import { cpSync } from 'node:fs';

import { defineConfig } from 'tsup';

// Build-time licensing URL — passed via env vars only at build time, baked
// into the bundle by `define`. See src/licensing/endpoint.ts.
const licenseEndpointEncoded = JSON.stringify(process.env.LICENSE_ENDPOINT_ENCODED ?? '');
const licenseEndpointXorKey = JSON.stringify(process.env.LICENSE_ENDPOINT_XOR_KEY ?? '');

export default defineConfig({
  entry: ['src'],
  outDir: 'dist',
  splitting: false,
  sourcemap: true,
  clean: true,
  minify: true,
  format: ['cjs', 'esm'],
  define: {
    __LICENSE_ENDPOINT_ENCODED__: licenseEndpointEncoded,
    __LICENSE_ENDPOINT_XOR_KEY__: licenseEndpointXorKey,
  },
  onSuccess: async () => {
    cpSync('src/utils/translations', 'dist/translations', { recursive: true });
  },
  loader: {
    '.json': 'file',
    '.yml': 'file',
  },
});
