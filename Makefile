export UV_CACHE_DIR ?= $(CURDIR)/.local/uv-cache

.PHONY: install build test simulate protocol
install:
	uv sync --frozen --extra dev
	npm --prefix web ci --cache $(CURDIR)/.local/npm-cache
build:
	npm --prefix web run build
test:
	uv run --frozen pytest -q
	npm --prefix web test
simulate:
	uv run --frozen bitmesh simulate --parameters 100
protocol:
	uv run --frozen python -m grpc_tools.protoc -I protocol --python_out=bitmesh --grpc_python_out=bitmesh protocol/mesh.proto
	sed -i 's/^import mesh_pb2 as mesh__pb2/from bitmesh import mesh_pb2 as mesh__pb2/' bitmesh/mesh_pb2_grpc.py
