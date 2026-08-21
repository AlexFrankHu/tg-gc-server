"""Verify tl_compat against real bytes captured from production node logs.

Run on a node after deployment: python3 script/tl_compat_selftest.py
"""
import os
import struct
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from telethon import TelegramClient
from telethon.extensions import BinaryReader
from telethon.tl.types import Dialog, PeerNotifySettings, PeerUser, User, UserStatusOffline

import tl_compat

tl_compat.install()

# Sample 1: users.getUsers(self) answered at layer ~148 (from 43.163.96.192 app.log)
S1 = b'(\xc6\x97\x8fS\x04\x00\x02\x00\x00\x00\x00q^/\xf7\x01\x00\x00\x00\xa0\x0e\xd3\x18T98P\x0fHo\xc3\xa0ng Kh\xc6\xb0\xc6\xa1ng\x0b84814418818?p\x8c\x00(A\x87j'
# Sample 2: contacts.resolvePhone answered at layer ~148 (user reported error)
S2 = b"(\xc6\x97\x8fG\x00\x00\x02\x00\x00\x00\x00\xb8:\xbfM\x00\x00\x00\x00\xfc\xc1\x18o\x9d\xd9\xdd\x02\x03Nam\x05Phung\x00\x00?p\x8c\x00\x8b'\x87j"


def read(data):
    r = BinaryReader(data)
    obj = r.tgread_object()
    left = len(data) - r.tell_position()
    return obj, left


u1, left1 = read(S1)
assert isinstance(u1, User), u1
assert (u1.id, u1.first_name, u1.phone, u1.is_self) == (8442044017, "Hoàng Khương", "84814418818", True), u1
assert isinstance(u1.status, UserStatusOffline)
assert u1.access_hash == 5780433155086552736
assert left1 == 0, left1

u2, left2 = read(S2)
assert isinstance(u2, User), u2
assert (u2.id, u2.first_name, u2.last_name) == (1304378040, "Nam", "Phung"), u2
assert u2.access_hash == 206560428125897212
assert left2 == 0, left2

# Round-trip check: a modern user serialised by Telethon still reads back unchanged.
modern = User(id=7, access_hash=8, first_name="a", last_name="b", username="c", phone="9",
              premium=True, bot=False, contact=True)
back, left = read(bytes(modern))
assert left == 0
assert (back.id, back.access_hash, back.first_name, back.last_name, back.username, back.phone) == \
    (7, 8, "a", "b", "c", "9")
assert back.premium is True and back.contact is True

# user#020b1422 (layer 199~216): flags2 fields must be consumed with their own types.
legacy214 = struct.pack('<II', 0x020B1422, 0x1 | 0x2) + struct.pack('<I', 0x20 | 0x1000) \
    + struct.pack('<q', 42) + struct.pack('<q', 99) + b'\x03abc' + struct.pack('<ii', 5, 7)
u3, left3 = read(legacy214)
assert isinstance(u3, User), u3
assert (u3.id, u3.access_hash, u3.first_name) == (42, 99, 'abc'), u3
assert (u3.stories_max_id, u3.bot_active_users) == (5, 7), u3
assert left3 == 0, left3

# dialog#d58a08c6: build the legacy payload and check every field lands correctly.
peer = bytes(PeerUser(user_id=123))
notify = bytes(PeerNotifySettings())
legacy = struct.pack('<II', 0xD58A08C6, 0x4 | 0x1 | 0x20) + peer + struct.pack('<6i', 11, 12, 13, 14, 15, 16) \
    + notify + struct.pack('<i', 77) + struct.pack('<i', 60)
d, left = read(legacy)
assert isinstance(d, Dialog), d
assert (d.peer.user_id, d.top_message, d.read_inbox_max_id, d.read_outbox_max_id) == (123, 11, 12, 13)
assert (d.unread_count, d.unread_mentions_count, d.unread_reactions_count) == (14, 15, 16)
assert d.pinned is True and d.pts == 77 and d.ttl_period == 60 and d.folder_id is None
assert d.unread_poll_votes_count == 0
assert left == 0, left

# dialog#a8edd0f5 (no ttl_period)
legacy2 = struct.pack('<II', 0xA8EDD0F5, 0x4) + peer + struct.pack('<6i', 11, 12, 13, 14, 15, 16) + notify
d2, left = read(legacy2)
assert isinstance(d2, Dialog) and d2.ttl_period is None and left == 0

# The reconnect patch must be installed on the class actually used by TelegramClient.
assert TelegramClient._handle_auto_reconnect.__module__ == "tl_compat", TelegramClient._handle_auto_reconnect

print("all tl_compat tests passed")
