# Python fstring-based template interpolation.

## Installation

You can install from [pypi](https://pypi.org/project/partial_fstring/)

```console
pip install -U partial_fstring
```

## Usage

```python
from partial_fstring import parse, render

block = parse("<{a}>{b}")
block.render({"b": 1}) # get '1'

# OR
render("<{a}>{b}", {"b": 1})
```

> Syntax Overview:

1. A string surrounded by "<" and ">" is called a block, and blocks can be nested, e.g., "<...>".
2. The string inside a block is treated as a Python f-string for interpolation.
    If interpolation fails, the block returns an empty string "".
3. Blocks can have names (similar to capture groups in regular expressions), placed inside "{{" and "}}". The syntax is "<{{name}}...>", and the value of this block can be referenced repeatedly using "{name}".
4. Named blocks can be defined without output, allowing later references. The syntax is "<?{{name}}...>".
5. If you want to use "<" and ">", but not as block indicators, write them as "\<" and "\>".
6. Supports the binary operator "||". The syntax is "part1||part2". 
    If ``part1`` doesn't raise an error, its value will be used; otherwise, ``part2`` is executed, and its value
    will be used or raises an error. This operator can be chained indefinitely.

Some more detailed tests show below:

```console
>>> from partial_fstring import render
>>> class mdict(dict):
...     @staticmethod
...     def __missing__(key):
...         return "{%s}" % key
>>> s = "{title}< ({year})>< [tmdbid={tmdbid}]>/Season {season}/{title} - {season_episode}<-{part}>< - 第 {episode} 集>< - {videoFormat}><.{edition}><.{videoCodec}><.{audioCodec}><-{releaseGroup}>{fileExt}"
>>> render(s, mdict())
'{title} ({year}) [tmdbid={tmdbid}]/Season {season}/{title} - {season_episode}-{part} - 第 {episode} 集 - {videoFormat}.{edition}.{videoCodec}.{audioCodec}-{releaseGroup}{fileExt}'

>>> s = "{title}< ({year})>||< [tmdbid={tmdbid}]>/Season {season}/{title} - {season_episode}<-{part}>< - 第 {episode} 集>< - {videoFormat}><.{edition}><.{videoCodec}><.{audioCodec}><-{releaseGroup}>{fileExt}"
>>> render(s, mdict())
'{title} ({year})'

>>> s = """{1} {f"{1} {2}"} { {3} }"""
>>> render(s, mdict())
'1 1 2 {3}'

>>> f"""{1} {f"{1} {2}"} { {3} }"""
'1 1 2 {3}'
```
