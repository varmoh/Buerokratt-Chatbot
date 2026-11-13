import { describe, expect, it } from 'vitest';

// eslint-disable-next-line @typescript-eslint/no-require-imports
const logger = require('./logger.js');

describe('logger.sanitize', () => {
  it('redacts secrets but keeps a correlatable fingerprint', () => {
    const out = logger.sanitize({
      headers: { cookie: 'customJwtCookie=abc', authorization: 'Bearer x' },
      azure_client_secret: 's3cret',
      max_tokens: 4096,
    });
    expect(out.headers.cookie).toMatch(/^\[REDACTED fp=[0-9a-f]{10}\]$/);
    expect(out.headers.authorization).toMatch(/^\[REDACTED/);
    expect(out.azure_client_secret).toMatch(/^\[REDACTED/);
    expect(out.max_tokens).toBe(4096);
    expect(JSON.stringify(out)).not.toContain('s3cret');
  });

  it('redacts data_sources authentication keys', () => {
    const out = logger.sanitize({ data_sources: [{ parameters: { authentication: { type: 'api_key', key: 'K' } } }] });
    expect(out.data_sources[0].parameters.authentication.key).toMatch(/^\[REDACTED/);
  });

  it('truncates long strings and survives cycles', () => {
    const obj: Record<string, unknown> = { text: 'x'.repeat(5000) };
    obj.self = obj;
    const out = logger.sanitize(obj);
    expect(out.text.length).toBeLessThan(2100);
    expect(out.self).toBe('[Circular]');
  });

  it('serializes errors with cause', () => {
    const cause = Object.assign(new Error('connect ECONNREFUSED'), { code: 'ECONNREFUSED' });
    const err = new TypeError('fetch failed', { cause });
    const out = logger.sanitize({ error: err });
    expect(out.error.message).toBe('fetch failed');
    expect(out.error.cause.code).toBe('ECONNREFUSED');
  });
});

describe('logger.maskUrl', () => {
  it('hides credentials and secret query params', () => {
    expect(logger.maskUrl('https://admin:admin@opensearch:9200')).toBe('https://***@opensearch:9200');
    expect(logger.maskUrl('https://x/y?api-key=abc&a=1')).toBe('https://x/y?api-key=***&a=1');
  });
});

describe('logger.cleanId', () => {
  it('strips line breaks so ids cannot forge log lines', () => {
    expect(logger.cleanId('abc\nFAKE')).toBe('abcFAKE');
  });
});
