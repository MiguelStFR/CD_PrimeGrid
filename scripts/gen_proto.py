"""Execute com o Python do ambiente virtual na raiz do projeto."""

from pathlib import Path

from grpc_tools import protoc

root = Path(__file__).resolve().parents[1]
raise SystemExit(protoc.main([
    "grpc_tools.protoc",
    f"-I{root}",
    f"--python_out={root}",
    f"--grpc_python_out={root}",
    str(root / "primegrid/proto/primegrid.proto"),
]))
