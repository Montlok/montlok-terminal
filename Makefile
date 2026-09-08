.PHONY: check web-test web-build rust-test native-configure native-build

check: web-test rust-test

web-test:
	npm --prefix apps/web test
	npm --prefix apps/web run tsc

web-build:
	npm --prefix apps/web run build

rust-test:
	cargo test --workspace

native-configure:
	cmake --preset macos-debug

native-build:
	cmake --build --preset macos-debug
