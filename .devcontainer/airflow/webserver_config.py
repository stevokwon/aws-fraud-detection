# webserver_config.py for GitHub Codespaces
from flask_appbuilder.security.manager import AUTH_DB

# Basic Auth setup
AUTH_TYPE = AUTH_DB
AUTH_ROLE_ADMIN = 'Admin'
AUTH_USER_REGISTRATION = True

# Codespaces compatibility
SESSION_COOKIE_SAMESITE = "Lax"
SESSION_COOKIE_SECURE = True
ENABLE_PROXY_FIX = True
WTF_CSRF_ENABLED = False
FAB_API_ALLOW_DOMAINS = ["*github.dev"]  


