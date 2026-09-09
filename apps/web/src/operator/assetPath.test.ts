import { afterEach, describe, expect, it, vi } from 'vitest';
import { assetPath } from './assetPath';
afterEach(() => vi.unstubAllEnvs());
describe('installed terminal assets', () => {
  it('keeps APIs independent of the installed static path', () => {
    vi.stubEnv('MONTLOK_WEB_BASE', '/terminal-v2/');
    expect(assetPath('/popout.html')).toBe('/terminal-v2/popout.html');
    expect(assetPath('vendor/perspective-5.3.1/cdn/perspective.js')).toBe(
      '/terminal-v2/vendor/perspective-5.3.1/cdn/perspective.js',
    );
  });
  it('supports the main site installation', () => {
    vi.stubEnv('MONTLOK_WEB_BASE', '/');
    expect(assetPath('popout.html')).toBe('/popout.html');
  });
});
