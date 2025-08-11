1. 把所有其它涉及 http request 的模块进行罗列（这些模块，都增加一个 urlopen 函数，用来提供给 request wrappper）
2. 提供一个函数，实现统一的 request wrappper
    1. 要求被操作函数，只需要支持 method, url, data, headers（data 如果有其他名字，需要进行指出，例如 body 或 content）
    2. 说明是否需要被解压缩
    3. 说明返回的对象要么是 SupportsRead，要么是 Iterator（/AsyncIterator）
    4. 但像 requests 这样的模块，不符合 3，所以需要适配（`resp.iter_content(65536)`）
    5. 必须指出是否异步
    6. 被包装函数自己最好不要 raise_for_status
    7. 被包装函数最好不要重定向，但可以如此（那么包装函数的重定向会停止）
    8. raise_for_status 抛出统一的错误（类似 HTTPStatusError）


# 进行 unicode 解码
decoder = codecs.getincrementaldecoder(encoding)(errors="replace")
for chunk in iterator:
    yield decoder.decode(chunk)
yield decoder.decode(b"", final=True)

# 进行流式解压缩，参考
gzip.GZipFile
urllib3.HTTPResponse.raw.stream
