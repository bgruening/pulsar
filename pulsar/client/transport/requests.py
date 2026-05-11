import logging

import requests

try:
    import requests_toolbelt
except ImportError:
    requests_toolbelt = None  # type: ignore


log = logging.getLogger(__name__)


def post_file(url, path):
    if requests_toolbelt is not None:
        # Streaming multipart upload — avoids loading the whole file into memory.
        m = requests_toolbelt.MultipartEncoder(
            fields={'file': ('filename', open(path, 'rb'))}
        )
        response = requests.post(url, data=m, headers={'Content-Type': m.content_type})
    else:
        log.warning(
            "Posting %s without requests_toolbelt: the entire file will be loaded into memory. "
            "Install requests_toolbelt (or pycurl, and use the curl transport) for streaming uploads.",
            path,
        )
        with open(path, 'rb') as f:
            response = requests.post(url, files={'file': f})
    response.raise_for_status()


def get_file(url, path):
    r = requests.get(url, stream=True)
    r.raise_for_status()
    with open(path, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)
                f.flush()
