.PHONY: install
install:
	RUSTFLAGS="-C target-cpu=native" RUST_MIN_STACK=16777216 scripts/cargo-install-all.sh --validator-only --release-with-lto .
