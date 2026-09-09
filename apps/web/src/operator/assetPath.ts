/** Assets follow the installed terminal path; API and authentication remain same-origin. */
export function assetPath(path: string) {
  return `${process.env.MONTLOK_WEB_BASE || '/'}${path.replace(/^\//, '')}`;
}
