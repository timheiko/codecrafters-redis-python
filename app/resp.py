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
    elif isinstance(data, bytes):
        return f"${len(data)}".encode() + LINE_SEPARATOR + data
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
    n, offset = len(payload), 0
    decoded = []
    while offset < n:
        item, offset = __decode(payload, offset)
        if offset == n and not len(decoded):
            return item
        decoded.append(item)
    return decoded


def __decode(payload: bytes, offset: int = 0) -> tuple[any, int]:
    if offset == 0:
        log("payload <<<", payload)
    i = offset
    match payload[i : i + 1]:
        case b"*":
            new_line_sep_pos = payload.find(LINE_SEPARATOR, i + 1)
            length = int(payload[i + 1 : new_line_sep_pos].decode())
            i = new_line_sep_pos + len(LINE_SEPARATOR)
            contents = []
            for _ in range(length):
                if payload[i : i + 1] == b"$":
                    decoded, i = __decode(payload, i)
                    contents.append(decoded)
            return contents, i
        case b"+":
            new_line_sep_pos = payload.find(LINE_SEPARATOR, i + 1)
            return payload[i + 1 : new_line_sep_pos].decode(), new_line_sep_pos + len(
                LINE_SEPARATOR
            )
        case b":":
            new_line_sep_pos = payload.find(LINE_SEPARATOR, i + 1)
            return int(
                payload[i + 1 : new_line_sep_pos].decode()
            ), new_line_sep_pos + len(LINE_SEPARATOR)
        case b"$":
            text, i = decode_bulk_string(payload, i)
            return text, i
        case b"-":
            new_line_sep_pos = payload.find(LINE_SEPARATOR, i + 1)
            message = payload[i + 5 : new_line_sep_pos].decode()
            return ValueError(message), new_line_sep_pos + len(LINE_SEPARATOR)
        case _:
            raise Exception(f"Unknown data type: {chr(payload[i])}")


def decode_bulk_string(payload: bytes, offset: int) -> tuple[bytes, int]:
    if payload[offset : offset + 1] == "$".encode():
        new_line_sep_pos = payload.find(LINE_SEPARATOR, offset + 1)
        length = int(payload[offset + 1 : new_line_sep_pos])
        content_start = new_line_sep_pos + len(LINE_SEPARATOR)
        content_end = content_start + length
        content = payload[content_start:content_end]
        try:
            text = content.decode()
            return (text, content_end + len(LINE_SEPARATOR))
        except UnicodeDecodeError as e:
            log("it must be RDB!", e)
            log("content length >>>", length, content)
            return (content, content_end)

    raise Exception(f"Cannot parse text from payload at offset {offset}: {payload}")


def decode_commands(data: bytes):
    n, offset, commands = len(data), 0, []
    while offset < n:
        command, next_offset = __decode(data, offset)
        commands.append((command, next_offset - offset))
        offset = next_offset
    return commands
