"""Classification of Telegram error texts that must change an account's flags.

Two account-level outcomes:
  restricted (is_restricted=1) — the account can no longer act on strangers.
  frozen     (is_frozen=1)     — Telegram froze the account; it is also restricted.

Telegram never reports the freeze directly on sending: sendMessage answers
PEER_ID_INVALID while resolvePhone answers FROZEN_METHOD_INVALID, so both texts
are matched here.
"""

# messages.sendMessage on a frozen/limited account -> PEER_ID_INVALID
RESTRICT_ERROR_MARKERS = (
    'PEER_ID_INVALID',
    'An invalid Peer was used',
)

# contacts.resolvePhone (and other active methods) on a frozen account
FROZEN_ERROR_MARKERS = (
    'FROZEN_METHOD_INVALID',
    'not available for frozen accounts',
)


def is_frozen_error(err: str) -> bool:
    return any(marker in err for marker in FROZEN_ERROR_MARKERS)


def is_restrict_error(err: str) -> bool:
    return any(marker in err for marker in RESTRICT_ERROR_MARKERS)
