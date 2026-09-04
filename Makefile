.PHONY: default setup build test fmt clippy check run-demo run-prod soak
default:  ; @just --list 2>/dev/null || echo "targets: build test fmt clippy check run-demo run-prod soak"
setup:    ; git config core.hooksPath .githooks && echo "git hooks installed from .githooks/"
build:    ; cargo build --workspace --all-targets
test:     ; cargo test --workspace -- --skip delta_spec_
spec:     ; cargo test --workspace delta_spec_
fmt:      ; cargo fmt --all
clippy:   ; cargo clippy --workspace --all-targets -- -D warnings
check:    ; cargo fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace
run-demo: ; KALSHI_ENV=demo cargo run --release --bin capture -- --env demo
run-prod: ; KALSHI_ENV=prod cargo run --release --bin capture -- --env prod --i-understand-this-is-production
soak:     ; KALSHI_ENV=prod cargo run --release --bin capture -- --env prod --i-understand-this-is-production --series KXBTCD --series KXETHD
