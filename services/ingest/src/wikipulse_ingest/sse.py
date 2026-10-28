import codecs
from collections.abc import Iterator
from dataclasses import dataclass

import httpx

from wikipulse_common.http import parse_retry_after


@dataclass(frozen=True)
class ServerSentEvent:
    data: str
    event: str
    id: str | None


class StreamHttpError(RuntimeError):
    def __init__(self, status: int, retry_after: float | None) -> None:
        super().__init__(f"EventStreams responded {status}")
        self.status = status
        self.retry_after = retry_after


class SseParser:
    """Incremental parser for the text/event-stream format (WHATWG HTML, section 9.2.6)."""

    def __init__(self) -> None:
        self._decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        self._buffer = ""
        self._data: list[str] = []
        self._event = ""
        self.last_event_id: str | None = None
        self.retry_ms: int | None = None

    def feed(self, chunk: bytes) -> list[ServerSentEvent]:
        self._buffer += self._decoder.decode(chunk)
        events = []
        while True:
            cut = _line_end(self._buffer)
            if cut is None:
                break
            line, rest = cut
            self._buffer = rest
            event = self._process_line(line)
            if event is not None:
                events.append(event)
        return events

    def _process_line(self, line: str) -> ServerSentEvent | None:
        if line == "":
            return self._dispatch()
        if line.startswith(":"):
            return None
        field, sep, value = line.partition(":")
        if sep and value.startswith(" "):
            value = value[1:]
        if field == "data":
            self._data.append(value)
        elif field == "event":
            self._event = value
        elif field == "id":
            if "\0" not in value:
                self.last_event_id = value
        elif field == "retry" and value.isdigit():
            self.retry_ms = int(value)
        return None

    def _dispatch(self) -> ServerSentEvent | None:
        data, event = self._data, self._event
        self._data, self._event = [], ""
        if not data:
            return None
        return ServerSentEvent(data="\n".join(data), event=event or "message", id=self.last_event_id)


def _line_end(buffer: str) -> tuple[str, str] | None:
    for index, char in enumerate(buffer):
        if char == "\n":
            return buffer[:index], buffer[index + 1 :]
        if char == "\r":
            # A trailing CR may be the first half of CRLF split across chunks.
            if index == len(buffer) - 1:
                return None
            skip = 2 if buffer[index + 1] == "\n" else 1
            return buffer[:index], buffer[index + skip :]
    return None


def read_event_stream(
    client: httpx.Client, url: str, user_agent: str, last_event_id: str | None
) -> Iterator[ServerSentEvent]:
    headers = {"Accept": "text/event-stream", "User-Agent": user_agent}
    if last_event_id is not None:
        headers["Last-Event-ID"] = last_event_id
    with client.stream("GET", url, headers=headers) as response:
        if response.status_code != 200:
            retry_after = response.headers.get("Retry-After")
            raise StreamHttpError(
                response.status_code, parse_retry_after(retry_after) if retry_after else None
            )
        parser = SseParser()
        for chunk in response.iter_bytes():
            yield from parser.feed(chunk)
