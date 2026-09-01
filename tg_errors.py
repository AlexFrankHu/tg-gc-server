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

# contacts.resolvePhone found no user for the phone number (PHONE_NOT_OCCUPIED).
# A single occurrence is a dead number; many in a row mean Telegram stopped
# resolving phones for this account, so the account itself is unusable.
NOT_OCCUPIED_ERROR_MARKERS = (
    'PHONE_NOT_OCCUPIED',
    'No user is associated to the specified phone',
    '号码未注册TG',
)


def is_frozen_error(err: str) -> bool:
    return any(marker in err for marker in FROZEN_ERROR_MARKERS)


def is_restrict_error(err: str) -> bool:
    return any(marker in err for marker in RESTRICT_ERROR_MARKERS)


def is_not_occupied_error(err: str) -> bool:
    return any(marker in err for marker in NOT_OCCUPIED_ERROR_MARKERS)
