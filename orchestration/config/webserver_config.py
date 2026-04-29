"""Airflow RBAC webserver configuration.

Implements:
- AUTH_TYPE = AUTH_DB (DB-backed user management)
- FAB RBAC roles: Admin, Viewer, User (data engineer), Op (operations)
- No anonymous access
- Session cookie security settings

All secret keys come from environment variables.
"""

from __future__ import annotations
import os

from flask_appbuilder.security.manager import AUTH_DB

# ── Authentication ────────────────────────────────────────────────────
AUTH_TYPE = AUTH_DB
AUTH_USER_REGISTRATION = False           # Disable public self-registration
AUTH_USER_REGISTRATION_ROLE = "Viewer"   # Default role if enabled

# ── Roles mapping (must match role names in Airflow DB) ───────────────
AUTH_ROLES_MAPPING = {
    "Admin":  ["Admin"],
    "User":   ["User"],   # Data engineers: trigger DAGs, view logs
    "Op":     ["Op"],     # Operations: manage task instances
    "Viewer": ["Viewer"], # Read-only: view DAGs, task status
}

# ── Security ──────────────────────────────────────────────────────────
SECRET_KEY = os.environ["AIRFLOW_SECRET_KEY"]
WTF_CSRF_ENABLED = True
PERMANENT_SESSION_LIFETIME = 60 * 60 * 8   # 8 hours
SESSION_COOKIE_SECURE = False               # True in production (HTTPS)
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = "Lax"

# ── Branding ──────────────────────────────────────────────────────────
APP_NAME = "Fintech Analytics – Airflow"
APP_ICON = ""
APP_THEME = ""
