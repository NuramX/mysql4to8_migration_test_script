"""
Minimal MySQL 4.0 connector using raw sockets.
MySQL 4.0 uses protocol version 10 but old password scrambling (pre-4.1).
"""

import socket
import struct
import csv
import io


def _old_password_hash(password: str) -> tuple[int, int]:
    nr, add, nr2 = 1345345333, 7, 0x12345671
    for c in password:
        if c in (' ', '\t'):
            continue
        tmp = ord(c)
        nr ^= (((nr & 63) + add) * tmp) + (nr << 8)
        nr2 += (nr2 << 8) ^ nr
        add += tmp
    return (nr & 0x7FFFFFFF), (nr2 & 0x7FFFFFFF)


def _scramble_old(password: str, seed: str) -> bytes:
    if not password:
        return b'\x00'
    MAX = 0x3FFFFFFF
    pw = _old_password_hash(password)
    ms = _old_password_hash(seed)
    s1 = (pw[0] ^ ms[0]) % MAX
    s2 = (pw[1] ^ ms[1]) % MAX
    result = []
    for _ in seed:
        s1 = (s1 * 3 + s2) % MAX
        s2 = (s1 + s2 + 33) % MAX
        result.append(int(s1 * 31 / MAX) + 64)
    s1 = (s1 * 3 + s2) % MAX
    s2 = (s1 + s2 + 33) % MAX
    extra = int(s1 * 31 / MAX)
    return bytes([c ^ extra for c in result]) + b'\x00'


def _read_packet(sock: socket.socket) -> bytes:
    header = b''
    while len(header) < 4:
        header += sock.recv(4 - len(header))
    length = struct.unpack_from('<I', header[:3] + b'\x00')[0]
    data = b''
    while len(data) < length:
        chunk = sock.recv(length - len(data))
        if not chunk:
            raise ConnectionError("Connection closed by server")
        data += chunk
    return data


def _send_packet(sock: socket.socket, data: bytes, seq: int) -> None:
    header = struct.pack('<I', len(data))[:3] + bytes([seq])
    sock.sendall(header + data)


class MySQL40Connection:
    def __init__(self, host: str, port: int, user: str,
                 password: str, database: str, timeout: int = 30,
                 charset: str = 'latin1'):
        self._sock    = socket.create_connection((host, port), timeout=timeout)
        self._sock.settimeout(timeout)
        self._seq     = 0
        self._charset = charset
        self._connect(user, password, database)

    def _connect(self, user: str, password: str, database: str) -> None:
        greeting = _read_packet(self._sock)

        # Parse greeting: skip protocol_version(1) + server_version(n+1) + thread_id(4)
        pos = 1
        end = greeting.index(b'\x00', pos)
        pos = end + 1 + 4  # skip server_version null-term + thread_id

        salt = greeting[pos:pos + 8].decode('latin1')
        pos += 8 + 1  # salt + null

        # capabilities (2 bytes) — MySQL 4.0 does NOT set CLIENT_PROTOCOL_41
        caps = struct.unpack_from('<H', greeting, pos)[0]

        # Build old-style handshake response
        CLIENT_LONG_PASSWORD  = 0x0001
        CLIENT_CONNECT_WITH_DB = 0x0008
        client_caps = CLIENT_LONG_PASSWORD | CLIENT_CONNECT_WITH_DB

        scrambled = _scramble_old(password, salt)

        packet = (
            struct.pack('<H', client_caps) +       # client capabilities (2 bytes)
            struct.pack('<I', 16777216)[:3] +       # max_allowed_packet (3 bytes)
            user.encode('latin1') + b'\x00' +       # username + null
            scrambled +                             # scrambled password + null
            database.encode('latin1') + b'\x00'    # database + null
        )
        _send_packet(self._sock, packet, 1)

        response = _read_packet(self._sock)
        if response[0] == 0xFF:
            errno  = struct.unpack_from('<H', response, 1)[0]
            errmsg = response[3:].decode('latin1', errors='replace')
            raise ConnectionError(f"MySQL error {errno}: {errmsg}")

    def query(self, sql: str) -> list[tuple]:
        """Execute SELECT and return all rows as list of tuples."""
        self._seq = 0
        cmd = b'\x03' + sql.encode(self._charset, errors='replace')
        _send_packet(self._sock, cmd, 0)

        # Read result set header
        header_pkt = _read_packet(self._sock)
        if header_pkt[0] == 0xFF:
            errno  = struct.unpack_from('<H', header_pkt, 1)[0]
            errmsg = header_pkt[3:].decode('latin1', errors='replace')
            raise RuntimeError(f"Query error {errno}: {errmsg}")

        num_cols = header_pkt[0]

        # Read column definitions
        for _ in range(num_cols):
            _read_packet(self._sock)

        # EOF packet
        eof = _read_packet(self._sock)

        # Read rows
        rows: list[tuple] = []
        while True:
            row_pkt = _read_packet(self._sock)
            if row_pkt[0] == 0xFE and len(row_pkt) < 9:  # EOF
                break
            row = []
            pos = 0
            for _ in range(num_cols):
                if row_pkt[pos] == 0xFB:
                    row.append(None)
                    pos += 1
                else:
                    length = row_pkt[pos]
                    pos += 1
                    if length >= 0xFC:
                        if length == 0xFC:
                            length = struct.unpack_from('<H', row_pkt, pos)[0]
                            pos += 2
                        elif length == 0xFD:
                            length = struct.unpack_from('<I', row_pkt[pos:pos+3] + b'\x00')[0]
                            pos += 3
                    val = row_pkt[pos:pos + length].decode(self._charset, errors='replace')
                    row.append(val)
                    pos += length
            rows.append(tuple(row))
        return rows

    def query_with_cols(self, sql: str) -> tuple[list[str], list[tuple]]:
        """Execute SELECT and return (column_names, rows)."""
        self._seq = 0
        cmd = b'\x03' + sql.encode(self._charset, errors='replace')
        _send_packet(self._sock, cmd, 0)

        header_pkt = _read_packet(self._sock)
        if header_pkt[0] == 0xFF:
            errno  = struct.unpack_from('<H', header_pkt, 1)[0]
            errmsg = header_pkt[3:].decode('latin1', errors='replace')
            raise RuntimeError(f"Query error {errno}: {errmsg}")

        num_cols = header_pkt[0]

        columns: list[str] = []
        for _ in range(num_cols):
            col_pkt = _read_packet(self._sock)
            pos = 0
            tlen = col_pkt[pos]; pos += 1 + tlen          # skip table name
            clen = col_pkt[pos]; pos += 1                  # column name length
            col_name = col_pkt[pos:pos + clen].decode('latin1', errors='replace')
            columns.append(col_name)

        _read_packet(self._sock)  # EOF

        rows: list[tuple] = []
        while True:
            row_pkt = _read_packet(self._sock)
            if row_pkt[0] == 0xFE and len(row_pkt) < 9:
                break
            row = []
            pos = 0
            for _ in range(num_cols):
                if row_pkt[pos] == 0xFB:
                    row.append(None)
                    pos += 1
                else:
                    length = row_pkt[pos]
                    pos += 1
                    if length >= 0xFC:
                        if length == 0xFC:
                            length = struct.unpack_from('<H', row_pkt, pos)[0]
                            pos += 2
                        elif length == 0xFD:
                            length = struct.unpack_from('<I', row_pkt[pos:pos+3] + b'\x00')[0]
                            pos += 3
                    val = row_pkt[pos:pos + length].decode(self._charset, errors='replace')
                    row.append(val)
                    pos += length
            rows.append(tuple(row))
        return columns, rows

    def query_stream(self, sql: str):
        """Execute SELECT and yield rows one by one as tuples."""
        self._seq = 0
        cmd = b'\x03' + sql.encode(self._charset, errors='replace')
        _send_packet(self._sock, cmd, 0)

        # Read result set header
        header_pkt = _read_packet(self._sock)
        if header_pkt[0] == 0xFF:
            errno  = struct.unpack_from('<H', header_pkt, 1)[0]
            errmsg = header_pkt[3:].decode('latin1', errors='replace')
            raise RuntimeError(f"Query error {errno}: {errmsg}")

        num_cols = header_pkt[0]

        # Read column definitions
        for _ in range(num_cols):
            _read_packet(self._sock)

        # EOF packet
        _read_packet(self._sock)

        # Yield rows
        while True:
            row_pkt = _read_packet(self._sock)
            if row_pkt[0] == 0xFE and len(row_pkt) < 9:  # EOF
                break
            row = []
            pos = 0
            for _ in range(num_cols):
                if row_pkt[pos] == 0xFB:
                    row.append(None)
                    pos += 1
                else:
                    length = row_pkt[pos]
                    pos += 1
                    if length >= 0xFC:
                        if length == 0xFC:
                            length = struct.unpack_from('<H', row_pkt, pos)[0]
                            pos += 2
                        elif length == 0xFD:
                            length = struct.unpack_from('<I', row_pkt[pos:pos+3] + b'\x00')[0]
                            pos += 3
                    val = row_pkt[pos:pos + length].decode(self._charset, errors='replace')
                    row.append(val)
                    pos += length
            yield tuple(row)

    def close(self) -> None:
        try:
            _send_packet(self._sock, b'\x01', 0)
        except Exception:
            pass
        self._sock.close()
