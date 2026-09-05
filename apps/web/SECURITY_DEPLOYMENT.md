# Tokyo operator HTTPS deployment — 2026-09-05

Public entry: https://tokyo.montlok.com/login

## Activated controls

- Cloudflare DNS-only A record to 64.83.36.66. No Cloudflare Tunnel, WARP, VPN encapsulation, or change to the server's OKX egress.
- Ubuntu repository Nginx and Certbot. TLS 1.2/1.3, authenticated ECDHE ciphers, session tickets disabled, HSTS. Invalid server names rejected. HTTP only serves ACME validation and HTTPS redirect.
- Nginx forwards to 127.0.0.1:18081 and overwrites proxy identity headers. This backend port is not opened in the firewall.
- Independent operator password stored as a salted scrypt hash (N=131072, r=8, p=1), outside the application-writable directory. Passwords and exchange credentials are independent.
- Public cookie uses the `__Host-` prefix, Secure, HttpOnly and SameSite=Strict. Sessions expire after 8 hours, with a 30-minute idle limit; logout revokes sessions and unexecuted confirmations. Open authenticated streams also close on revocation.
- Same-origin checks, CSRF checks, login throttling, no-store private API responses, CSP, anti-framing and no-referrer headers. CSP allows inline styles required by Ant's CSS-in-JS, but does not allow inline/eval scripts.
- Exchange credentials remain in an authenticated-encryption Fernet vault. No API secret is returned to the browser. The duplicate original Demo `.env` was retired only after an exact value comparison against the vault and a successful encrypted-backup recovery test.
- The vault's local wrapping key is protected by file permissions. This is **not a remote KMS/HSM** and cannot protect against a compromised root account or an attacker controlling the service process.
- Writes require a session-bound, profile-version-bound confirmation, valid for 90 seconds. Repeated execution reuses the stored receipt, and receipts cannot be replayed across sessions. Live profiles remain read-only. The existing no-leverage/no-borrowing restriction remains active.
- Redacted application audit records; Nginx access logs exclude query strings and request bodies. Authentication cookies and passwords are not logged.

## Recovery and renewal

- Certificate initially issued by DNS-01, expiring 2026-12-04. Renewal was reconfigured to webroot HTTP-01 and its simulated renewal succeeded.
- `certbot.timer` checks renewal automatically. `/etc/letsencrypt/renewal-hooks/deploy/nautilus-nginx` validates Nginx before reloading the new certificate.
- `nautilus-operator-backup.timer` creates an age-encrypted archive daily at 18:00 UTC plus up to five minutes of jitter (02:00–02:05 Beijing time).
- Backup contains the vault, wrapping key, password hash, a consistent SQLite backup and deployment configuration. Only the age **public recipient** is present on the server; the recovery identity is on the user's Mac.
- Recovery materials: `/Users/gabiri/Documents/量化/运营恢复材料/`, mode 0700. Files are outside the engine Git repository and restricted to the local user.
- The initial archive was copied to the Mac. Decryption, vault authentication, active Demo profile, login hash, SQLite `integrity_check` and Nginx configuration were verified in memory, without writing plaintext recovery files.
- Subsequent automatic archives currently remain on the Tokyo host. Automatic off-host replication and disaster-recovery failover are **not** configured. The user should retain the recovery identity in a separate protected backup location.
- Original Nginx default site and pre-HTTPS operator server source are retained under `/www/nautilus/operator/rollback-https-20260905/`. Restoring the old authentication behavior must never be done behind the public proxy.

## Verification performed

- 16 focused backend tests passed. Tests replace exchange execution and never trade.
- Public TLS chain and hostname accepted without disabling certificate validation.
- Anonymous `/api/session` and `/api/account` returned 401. Login issued a protected cookie. Authenticated Demo account, HTTPS SSE and WSS market data were received. Logout returned the account API to 401.
- TLS 1.1 handshake rejected by server with protocol-version alert. Unknown HTTP host closed without serving a site.
- Browser displayed the real HTTPS login page. Full logged-in browser workflow is not part of this verification; authenticated transport was checked programmatically.
- Frontend TypeScript and production build passed. The new login page uses Ant Design Pro's LoginForm.
- Existing Paper engine kept running as PID 132044 throughout this deployment. No exchange orders were submitted by deployment/security tests.

## Explicit remaining issues

1. The existing Baota installer replaces the official download origin with `kaka.ktflj.cn` and disables certificate checking. It was not executed. Source evidence is `/www/server/panel/install/install_soft.sh` and `/www/server/panel/class/panelPlugin.py`.
2. Baota's local-Nginx takeover UI still requires installing its panel-managed Nginx. The active site config is in `/www/server/panel/vhost/nginx/tokyo.montlok.com.conf`, included by Ubuntu Nginx, but the Baota website/reverse-proxy list has **not** been integrated. Official panel remediation needs a separate approved change.
3. MFA/WebAuthn, external KMS/HSM, a separate backup destination, independent host-trust review and multi-host failover remain outside this completed change. This is not a certification that the entire system meets a P0 financial production standard.
4. The private Paper backend and the OKX Demo execution account remain separate systems. HTTPS deployment does not turn the Paper run into exchange execution.

Official implementation references: [Nginx TLS](https://nginx.org/en/docs/http/ngx_http_ssl_module.html), [Nginx WebSocket proxying](https://nginx.org/en/docs/http/websocket.html), [age](https://github.com/FiloSottile/age).
