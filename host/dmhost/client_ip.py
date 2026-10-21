"""The IP address of the user behind the reverse proxy (Caddy).

In production, Caddy sends each request to the API, so the TCP peer of the API is Caddy.
Caddy puts the address of its own peer at the end of ``X-Forwarded-For``. A client can
send its own ``X-Forwarded-For`` with false addresses, and Caddy keeps them in front of
the true address, so only the entries at the end come from proxies that we trust.

The rule:

- If the TCP peer is not a trusted proxy, the peer is the user. The API ignores
  ``X-Forwarded-For``, so a user who connects directly cannot change the address.
- If the TCP peer is a trusted proxy, the API reads ``X-Forwarded-For`` from right to left
  and takes the first address that is not a trusted proxy.

``trusted_proxies`` holds IP addresses and CIDR networks (``DM_HOST_TRUSTED_PROXIES``).
"""

from __future__ import annotations

import ipaddress
from typing import Iterable, List, Optional, Sequence, Union

Network = Union[ipaddress.IPv4Network, ipaddress.IPv6Network]


def parse_networks(values: Iterable[str]) -> List[Network]:
    """Return the networks of ``values``. A plain address becomes a network of one address."""
    return [ipaddress.ip_network(value.strip(), strict=False) for value in values if value.strip()]


def client_ip(
    peer: Optional[str], forwarded_for: Optional[str], trusted_proxies: Sequence[Network]
) -> Optional[str]:
    """Return the address of the user, or ``None`` if the request has no peer address."""
    if peer is None or not _is_trusted(peer, trusted_proxies):
        return peer
    hops = [hop.strip() for hop in (forwarded_for or "").split(",") if hop.strip()]
    for hop in reversed(hops):
        if _parse(hop) is None:
            # A value that is not an address came from the client. Stop here, so that the
            # client cannot choose the address.
            break
        if not _is_trusted(hop, trusted_proxies):
            return hop
    return peer


def _parse(value: str) -> Optional[Union[ipaddress.IPv4Address, ipaddress.IPv6Address]]:
    try:
        return ipaddress.ip_address(value)
    except ValueError:
        return None


def _is_trusted(value: str, trusted_proxies: Sequence[Network]) -> bool:
    address = _parse(value)
    return address is not None and any(address in network for network in trusted_proxies)
