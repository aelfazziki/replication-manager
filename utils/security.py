from cryptography.fernet import Fernet
from flask import current_app

class SecurityManager:
    """Handles encryption and security-related tasks."""
    def __init__(self):
        self.cipher = None

    def init_app(self, app):
        """Initialize security manager with app context."""
        self.cipher = Fernet(app.config['ENCRYPTION_KEY'])

    def encrypt(self, data):
        """Encrypt sensitive data."""
        if isinstance(data, dict):
            return {k: self.cipher.encrypt(str(v).encode()).decode() for k, v in data.items()}
        return self.cipher.encrypt(str(data).encode()).decode()

    def decrypt(self, encrypted_data):
        """Decrypt sensitive data."""
        if isinstance(encrypted_data, dict):
            return {k: self.cipher.decrypt(v.encode()).decode() for k, v in encrypted_data.items()}
        return self.cipher.decrypt(encrypted_data.encode()).decode()

    def validate_credentials(self, endpoint):
        """Validate endpoint credentials."""
        try:
            with endpoint.get_connection() as conn:
                return conn.is_valid()
        except Exception:
            return False