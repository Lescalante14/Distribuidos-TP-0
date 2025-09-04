import logging

# Protocol constants
FIELD_SEPARATOR = 0x00    # Null byte separator
PAYLOAD_LENGTH_BYTES = 4  # 4 bytes for the length of the message
MAX_MESSAGE_SIZE = 1024 * 1024 * 2 # 2MB
RESPONSE_HEADER_SIZE = 2 # 2 bytes for the success flag and separator

class BetData:
    """Represents a lottery bet in binary format"""
    
    def __init__(self, nombre="", apellido="", dni="", nacimiento="", numero=""):
        self.nombre = nombre
        self.apellido = apellido
        self.dni = dni
        self.nacimiento = nacimiento
        self.numero = numero


class Response:
    """Represents server response"""
    
    def __init__(self, success=False, message=""):
        self.success = success
        self.message = message


class Protocol:
    """Handles binary communication protocol"""
    
    def __init__(self):
        pass
    
    def _int_to_bytes(self, value):
        """Convert integer to 4 bytes in big-endian format"""
        return bytes([
            (value >> 24) & 0xFF,
            (value >> 16) & 0xFF,
            (value >> 8) & 0xFF,
            value & 0xFF
        ])
    
    def _bytes_to_int(self, data):
        """Convert 4 bytes to integer in big-endian format"""
        return (data[0] << 24) | (data[1] << 16) | (data[2] << 8) | data[3]
    
    def _recv_exact(self, sock, n):
        """Receive exactly n bytes or return None if connection closed."""
        buf = bytearray()
        while len(buf) < n:
            chunk = sock.recv(n - len(buf))
            if not chunk:   # peer closed before full data
                return None
            buf.extend(chunk)
        return bytes(buf)
    
    def deserialize_bet(self, data):
        """Convert binary data to BetData"""
        if len(data) < 1: # At least 1 byte for the first field
            raise ValueError("data too short")
        
        bet = BetData()
        offset = 0
        
        # Read nombre
        nombre_end = self._find_next_separator(data, offset)
        if nombre_end == -1:
            raise ValueError("invalid nombre field")
        bet.nombre = data[offset:nombre_end].decode('utf-8')
        offset = nombre_end + 1
        
        # Read apellido
        apellido_end = self._find_next_separator(data, offset)
        if apellido_end == -1:
            raise ValueError("invalid apellido field")
        bet.apellido = data[offset:apellido_end].decode('utf-8')
        offset = apellido_end + 1
        
        # Read dni
        dni_end = self._find_next_separator(data, offset)
        if dni_end == -1:
            raise ValueError("invalid dni field")
        bet.dni = data[offset:dni_end].decode('utf-8')
        offset = dni_end + 1
        
        # Read nacimiento
        nacimiento_end = self._find_next_separator(data, offset)
        if nacimiento_end == -1:
            raise ValueError("invalid nacimiento field")
        bet.nacimiento = data[offset:nacimiento_end].decode('utf-8')
        offset = nacimiento_end + 1
        
        # Read numero (last field, no separator)
        bet.numero = data[offset:].decode('utf-8')
        
        return bet
    
    def _find_next_separator(self, data, offset):
        """Find the next field separator starting from offset"""
        for i in range(offset, len(data)):
            if data[i] == FIELD_SEPARATOR:
                return i
        return -1
    
    def send_message(self, sock, data):
        """Send a message using the protocol: length + data"""
        length = len(data)

        if length < 0 or length > MAX_MESSAGE_SIZE:
            return None
        
        # Send length (4 bytes, big endian)
        length_bytes = self._int_to_bytes(length)
        sock.sendall(length_bytes) #sendall is guaranteed to send the entire message
        
        # Send data
        sock.sendall(data) #sendall is guaranteed to send the entire message
    
    def receive_message(self, sock):
        """Receive a message using the protocol: length + data"""
        
        # Receive length (4 bytes)
        length_data = self._recv_exact(sock, PAYLOAD_LENGTH_BYTES)
        if length_data is None:
            return None
        
        length = self._bytes_to_int(length_data)

        if length < 0 or length > MAX_MESSAGE_SIZE:
            return None
        
        # Receive message data
        message_data = self._recv_exact(sock, length)
        if message_data is None:
            return None
        
        return message_data
    
    def serialize_response(self, resp):
        """Convert Response to binary format"""
        # Response format: [success][separator][message]
        message_bytes = resp.message.encode('utf-8')
        total_size = RESPONSE_HEADER_SIZE + len(message_bytes)  # success + separator + message bytes
        
        buffer = bytearray(total_size)
        offset = 0
        
        # Write success flag
        buffer[offset] = 0x01 if resp.success else 0x00
        offset += 1
        
        # Write separator
        buffer[offset] = FIELD_SEPARATOR
        offset += 1
        
        # Write message
        buffer[offset:offset + len(message_bytes)] = message_bytes
        
        return bytes(buffer)
