LINE_SEPARATOR = b"\r\n"


def encode(data: any) -> bytes:
    """
    Encode data into RESP format to send it to client
    https://redis-doc-test.readthedocs.io/en/latest/topics/protocol/
    """
    if isinstance(data, str):
        return (
            b"$"
            + str(len(data)).encode()
            + LINE_SEPARATOR
            + data.encode()
            + LINE_SEPARATOR
        )
    elif isinstance(data, int):
        return b":" + str(data).encode() + LINE_SEPARATOR
    elif data is None:
        return b"$-1" + LINE_SEPARATOR
    elif isinstance(data, list):
        return (
            b"*"
            + str(len(data)).encode()
            + LINE_SEPARATOR
            + b"".join(encode(item) for item in data)
        )
    else:
        raise Exception(f"Unsupported encoding data type: {type(data)}: {data}")


def encode_simple(data: str) -> bytes:
    """
    Encode a string into RESP simple string format to send it to client
    https://redis-doc-test.readthedocs.io/en/latest/topics/protocol/
    """
    if isinstance(data, str):
        return b"+" + data.encode() + LINE_SEPARATOR
    else:
        raise Exception(f"Unsupported encoding data type: {type(data)}: {data}")
