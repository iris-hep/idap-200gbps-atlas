import logging
from aiohttp import ClientError
import fsspec
from fsspec.implementations.http import (
    HTTPFile,
    HTTPFileSystem,
    HTTPStreamFile,
    sync,
    sync_wrapper,
)
from tenacity import retry, retry_if_exception_type, wait_random_exponential


def register_retry_http_filesystem():
    """Register the retry version of HTTPFileSystem and HTTPFile with fsspec."""
    fsspec.register_implementation("http", RetryHTTPFileSystem)
    fsspec.register_implementation("https", RetryHTTPFileSystem)


class RetryHTTPFile(HTTPFile):
    """Retry version of HTTPFile.

    Defers everything to the parent class except the range read, which is used by uproot.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.old_async_fetch_range = self.async_fetch_range

    @retry(
        wait=wait_random_exponential(multiplier=1, max=60),
        retry=retry_if_exception_type(ClientError),
    )
    def async_fetch_range(self, start, end):
        logging.debug(f"retry async_fetch_range: {start} {end}")
        return super().async_fetch_range(start, end)

    _fetch_range = sync_wrapper(async_fetch_range)


class RetryHTTPFileSystem(HTTPFileSystem):
    """Retry version of HTTPFileSystem."""

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=None,  # XXX: This differs from the base class.
        cache_type=None,
        cache_options=None,
        size=None,
        **kwargs,
    ):
        """Make a file-like object

        Parameters
        ----------
        path: str
            Full URL with protocol
        mode: string
            must be "rb"
        block_size: int or None
            Bytes to download in one request; use instance value if None. If
            zero, will return a streaming Requests file-like instance.
        kwargs: key-value
            Any other parameters, passed to requests calls
        """
        if mode != "rb":
            raise NotImplementedError
        block_size = block_size if block_size is not None else self.block_size
        kw = self.kwargs.copy()
        kw["asynchronous"] = self.asynchronous
        kw.update(kwargs)
        size = size or self.info(path, **kwargs)["size"]
        session = sync(self.loop, self.set_session)
        if block_size and size:
            return RetryHTTPFile(
                self,
                path,
                session=session,
                block_size=block_size,
                mode=mode,
                size=size,
                cache_type=cache_type or self.cache_type,
                cache_options=cache_options or self.cache_options,
                loop=self.loop,
                **kw,
            )
        else:
            return HTTPStreamFile(
                self,
                path,
                mode=mode,
                loop=self.loop,
                session=session,
                **kw,
            )
