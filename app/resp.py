from .log import log

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
    elif isinstance(data, ValueError):
        return f"-ERR {" ".join(data.args)}".encode() + LINE_SEPARATOR
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


def decode(payload: bytes):
    log("payload <<<", payload)
    n, i = len(payload), 0
    contents = []
    while i < n:
        if payload[i : i + 1] == b"*":
            new_line_sep_pos = payload.find(LINE_SEPARATOR, i + 1)
            length = int(payload[i + 1 : new_line_sep_pos].decode())
            i = new_line_sep_pos + len(LINE_SEPARATOR)
            for _ in range(length):
                if payload[i : i + 1] == b"$":
                    text, i = decode_bulk_string(payload, i)
                    contents.append(text)
        else:
            raise Exception(f"Unknown data type: {chr(payload[i])}")

    return contents


def decode_bulk_string(payload: bytes, offset: int) -> tuple[bytes, int]:
    if payload[offset : offset + 1] == "$".encode():
        new_line_sep_pos = payload.find(LINE_SEPARATOR, offset + 1)
        length = int(payload[offset + 1 : new_line_sep_pos])
        text_start = new_line_sep_pos + len(LINE_SEPARATOR)
        text_end = text_start + length
        text = payload[text_start:text_end].decode()
        return (text, text_end + len(LINE_SEPARATOR))

    raise Exception(f"Cannot parse text from payload at offset {offset}: {payload}")
