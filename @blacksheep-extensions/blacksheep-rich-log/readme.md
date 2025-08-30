# BlackSheep access log middleware.

## Installation

You can install from [pypi](https://pypi.org/project/blacksheep-rich-log/)

```console
pip install -U blacksheep-rich-log
```

## Usage

```python
from blacksheep import Application
from blacksheep_rich_log import middleware_access_log

app = Application()
middleware_access_log(app)
```
