# Python http request utils.

## Installation

You can install from [pypi](https://pypi.org/project/python-http_request/)

```console
pip install -U python-http_request
```

## Usage

```python
import http_request
import http_request.extension
```

## Extension

I've implemented several modules, all of which provide a ``request`` function. Their signatures are similar, so they can be used as drop-in replacements for each other.

1. [aiohttp_client_request](https://pypi.org/project/aiohttp_client_request/)
1. [aiosonic_request](https://pypi.org/project/aiosonic_request/)
1. [asks_request](https://pypi.org/project/asks_request/)
1. [blacksheep_client_request](https://pypi.org/project/blacksheep_client_request/)
1. [curl_cffi_request](https://pypi.org/project/curl_cffi_request/)
1. [http_client_request](https://pypi.org/project/http_client_request/)
1. [httpcore_request](https://pypi.org/project/httpcore_request/)
1. [httpx_request](https://pypi.org/project/httpx_request/)
1. [hyper_request](https://pypi.org/project/hyper_request/)
1. [pycurl_request](https://pypi.org/project/pycurl_request/)
1. [python-urlopen](https://pypi.org/project/python-urlopen/)
1. [requests_request](https://pypi.org/project/requests_request/)
1. [tornado_client_request](https://pypi.org/project/tornado_client_request/)
1. [urllib3_request](https://pypi.org/project/urllib3_request/)

To make it more general, I've encapsulated a ``request`` function

```python
from http_request.extension import request
```

You just need to implement a ``urlopen`` function pass to ``request``, then it can be directly extended. The ``urlopen`` function signature is roughly as follows:

```python
def urlopen[Response](
    url: str, 
    method: str,  
    data=None, 
    headers: None | dict[str, str] = None, 
    **request_args, 
) -> Response:
    ...
```
