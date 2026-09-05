# Upstream

This application vendors Ant Design Pro 6.0.3, commit
`adfd44085738ca953573a13322c1ba84aca8b9e3`, from
https://github.com/ant-design/ant-design-pro (MIT; retained in LICENSE).

Palantir Blueprint provides the dense operational controls and table components:
https://github.com/palantir/blueprint (Apache-2.0).

TradingView Lightweight Charts provides financial charts:
https://github.com/tradingview/lightweight-charts (Apache-2.0).

Ant Design Pro's Umi application, route model, ProLayout, account/form/table patterns,
and build pipeline are retained. Nautilus-specific pages and a local API bridge replace
the sample backend routes. The former Vue/FreqUI application remains in
`../operator_dashboard` while this independent application is verified.

No trading credentials are part of this source tree or frontend build.
