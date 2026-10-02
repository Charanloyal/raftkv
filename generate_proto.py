import os
import sys
from grpc_tools import protoc

def generate():
    project_dir = os.path.dirname(os.path.abspath(__file__))
    proto_dir = os.path.join(project_dir, "proto")
    out_dir = os.path.join(project_dir, "raftkv", "proto")
    os.makedirs(out_dir, exist_ok=True)
    
    # Touch __init__.py files
    open(os.path.join(project_dir, "raftkv", "__init__.py"), "a").close()
    open(os.path.join(out_dir, "__init__.py"), "a").close()

    proto_file = os.path.join(proto_dir, "raft.proto")
    
    cmd = [
        "grpc_tools.protoc",
        f"-I{proto_dir}",
        f"--python_out={out_dir}",
        f"--grpc_python_out={out_dir}",
        proto_file
    ]
    
    print(f"Generating gRPC code from {proto_file}...")
    res = protoc.main(cmd)
    if res != 0:
        print(f"Error generating gRPC code: {res}")
        sys.exit(res)
        
    # Fix import in raft_pb2_grpc.py for relative import compatibility
    grpc_py = os.path.join(out_dir, "raft_pb2_grpc.py")
    with open(grpc_py, "r") as f:
        content = f.read()
    
    content = content.replace("import raft_pb2 as raft__pb2", "from . import raft_pb2 as raft__pb2")
    
    with open(grpc_py, "w") as f:
        f.write(content)
        
    print("Successfully generated and patched gRPC python stubs.")

if __name__ == "__main__":
    generate()
