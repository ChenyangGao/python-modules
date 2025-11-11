# Python aria2 RPC call.

## Installation

You can install from [pypi](https://pypi.org/project/ariarpc/)

```console
pip install -U ariarpc
```

## Usage

```python
from ariarpc import AriaRPC, AriaXMLRPC

# JSON RPC
rpc = AriaRPC()
rpc.system.listMethods()

# asynchronous JSON RPC
await rpc.system.listMethods(async_=True)

# XML RPC
rpc = AriaXMLRPC()
rpc.system.listMethods()
```
