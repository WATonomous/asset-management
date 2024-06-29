import re
import requests
from urllib.parse import urlparse, parse_qs

RESOLVER_URL_PREFIXES = [
    "https://rgw.watonomous.ca/asset-perm",
    "https://rgw.watonomous.ca/asset-temp",
]


def extract_sha256(s):
    sha256_match = re.search(r"sha256:([a-f0-9]{64})", s)
    if not sha256_match:
        raise ValueError("Invalid string: does not contain a SHA-256 hash.")
    return sha256_match.group(1)


class WATcloudURI:
    def __init__(self, input_url):
        parsed_url = urlparse(input_url)
        if parsed_url.scheme != "watcloud":
            raise ValueError("Invalid WATCloud URI: protocol must be 'watcloud:'")
        if parsed_url.hostname != "v1":
            raise ValueError(
                f"Invalid WATCloud URI: unsupported version '{parsed_url.hostname}'. Only 'v1' is supported"
            )

        self.sha256 = extract_sha256(parsed_url.path)
        query_params = parse_qs(parsed_url.query)
        self.name = query_params.get("name", [None])[0]

    def resolve_to_url(self):
        for prefix in RESOLVER_URL_PREFIXES:
            url = f"{prefix}/{self.sha256}"
            response = requests.head(url)
            if response.ok:
                return url
        raise ValueError("Asset not found.")

    def __str__(self):
        return f"watcloud://v1/sha256:{self.sha256}?name={self.name}"
    
    def __repr__(self) -> str:
        return f"WATcloudURI({str(self)})"

    def __lt__(self, other):
        return self.sha256 < other.sha256

if __name__ == "__main__":
    # Example usage
    uri = WATcloudURI("watcloud://v1/sha256:906f98c1d660a70a6b36ad14c559a9468fe7712312beba1d24650cc379a62360?name=cloud-light.avif")
    print(uri.resolve_to_url())