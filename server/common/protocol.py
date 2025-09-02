import logging

# Protocol constants
ENDIANNESS_MARKER = 0x01  # Little endian marker
FIELD_SEPARATOR = 0x00    # Null byte separator


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
    
    
    def deserialize_bet(self, data):
        """Convert binary data to BetData"""
        if len(data) < 2:
            raise ValueError("data too short")
        
        # Check endianness marker
        if data[0] != ENDIANNESS_MARKER:
            raise ValueError("invalid endianness marker")
        
        bet = BetData()
        offset = 1  # Skip endianness marker
        
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
        
        # Send length (4 bytes, big endian)
        length_bytes = self._int_to_bytes(length)
        sock.sendall(length_bytes)
        
        # Send data
        sock.sendall(data)
    
    def receive_message(self, sock):
        """Receive a message using the protocol: length + data"""
        # Receive length (4 bytes)
        length_data = sock.recv(4)
        if len(length_data) < 4:
            return None
        
        length = self._bytes_to_int(length_data)
        
        # Receive message data
        message_data = b''
        while len(message_data) < length:
            chunk = sock.recv(length - len(message_data))
            if not chunk:
                return None
            message_data += chunk
        
        return message_data
    
    def serialize_response(self, resp):
        """Convert Response to binary format"""
        # Response format: [endianness][success][separator][message]
        message_bytes = resp.message.encode('utf-8')
        total_size = 1 + 1 + 1 + len(message_bytes)  # endianness + success + separator + message
        
        buffer = bytearray(total_size)
        offset = 0
        
        # Write endianness marker
        buffer[offset] = ENDIANNESS_MARKER
        offset += 1
        
        # Write success flag
        buffer[offset] = 0x01 if resp.success else 0x00
        offset += 1
        
        # Write separator
        buffer[offset] = FIELD_SEPARATOR
        offset += 1
        
        # Write message
        buffer[offset:offset + len(message_bytes)] = message_bytes
        
        return bytes(buffer)
