"""User database models for fleet dashboard authentication."""

import os
import sqlite3
from flask_login import UserMixin

_DEFAULT_DB = os.path.join(os.path.dirname(__file__), "fleet_users.db")
DB_PATH = os.environ.get("DB_PATH", _DEFAULT_DB)
# Ensure the parent directory exists (matters for mounted disks like /var/data)
_db_dir = os.path.dirname(DB_PATH)
if _db_dir and not os.path.exists(_db_dir):
    os.makedirs(_db_dir, exist_ok=True)


def get_db():
    """Get a database connection."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db():
    """Create tables if they don't exist. Seed default admin if no users."""
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pin TEXT UNIQUE NOT NULL,
            display_name TEXT NOT NULL,
            is_admin INTEGER DEFAULT 0,
            allowed_tabs TEXT DEFAULT 'all',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS user_vehicles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            vehicle_id TEXT NOT NULL,
            UNIQUE(user_id, vehicle_id)
        );
    """)

    # Migrate: if old schema has 'username' column, recreate table
    cols = [r[1] for r in conn.execute("PRAGMA table_info(users)").fetchall()]
    if "username" in cols and "pin" not in cols:
        print("Migrating users table from username/password to PIN...")
        conn.executescript("""
            DROP TABLE IF EXISTS users;
            DROP TABLE IF EXISTS user_vehicles;
            CREATE TABLE users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pin TEXT UNIQUE NOT NULL,
                display_name TEXT NOT NULL,
                is_admin INTEGER DEFAULT 0,
                allowed_tabs TEXT DEFAULT 'all',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE user_vehicles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                vehicle_id TEXT NOT NULL,
                UNIQUE(user_id, vehicle_id)
            );
        """)

    # Add allowed_tabs column if missing (for existing PIN-based DBs)
    if "allowed_tabs" not in cols and "pin" in cols:
        conn.execute("ALTER TABLE users ADD COLUMN allowed_tabs TEXT DEFAULT 'all'")

    # Seed default admin PIN (0000) if no users exist
    count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    if count == 0:
        conn.execute(
            "INSERT INTO users (pin, display_name, is_admin, allowed_tabs) VALUES (?, ?, ?, ?)",
            ("0000", "Administrator", 1, "all"),
        )
        print("Default admin created (PIN: 0000)")

    conn.commit()
    conn.close()


class User(UserMixin):
    """Flask-Login compatible user object."""

    def __init__(self, id, pin, display_name, is_admin, allowed_tabs):
        self.id = id
        self.pin = pin
        self.display_name = display_name or "User"
        self.is_admin = bool(is_admin)
        self.allowed_tabs = allowed_tabs or "all"

    def get_tab_list(self):
        """Return list of allowed tab keys, or None for all."""
        if self.is_admin or self.allowed_tabs == "all":
            return None  # None = all tabs
        return [t.strip() for t in self.allowed_tabs.split(",") if t.strip()]

    @staticmethod
    def get_by_id(user_id):
        conn = get_db()
        row = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
        conn.close()
        if row:
            return User(row["id"], row["pin"], row["display_name"],
                        row["is_admin"], row["allowed_tabs"])
        return None

    @staticmethod
    def get_by_pin(pin):
        conn = get_db()
        row = conn.execute("SELECT * FROM users WHERE pin = ?", (pin,)).fetchone()
        conn.close()
        if row:
            return User(row["id"], row["pin"], row["display_name"],
                        row["is_admin"], row["allowed_tabs"])
        return None


def get_allowed_vehicle_ids(user_id):
    """Return list of vehicle IDs assigned to a user, or None if admin."""
    user = User.get_by_id(user_id)
    if user and user.is_admin:
        return None  # None means "all vehicles"
    conn = get_db()
    rows = conn.execute(
        "SELECT vehicle_id FROM user_vehicles WHERE user_id = ?", (user_id,)
    ).fetchall()
    conn.close()
    return [r["vehicle_id"] for r in rows]


def get_all_users():
    """Return list of all users as dicts."""
    conn = get_db()
    rows = conn.execute(
        "SELECT id, pin, display_name, is_admin, allowed_tabs, created_at FROM users ORDER BY display_name"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def create_user(pin, display_name, is_admin=False, allowed_tabs="all"):
    """Create a new user. Returns user id."""
    conn = get_db()
    try:
        cur = conn.execute(
            "INSERT INTO users (pin, display_name, is_admin, allowed_tabs) VALUES (?, ?, ?, ?)",
            (pin, display_name, int(is_admin), allowed_tabs),
        )
        conn.commit()
        user_id = cur.lastrowid
    finally:
        conn.close()
    return user_id


def update_user(user_id, pin=None, display_name=None, is_admin=None, allowed_tabs=None):
    """Update user fields. Only non-None values are changed."""
    conn = get_db()
    try:
        if pin is not None:
            conn.execute("UPDATE users SET pin = ? WHERE id = ?", (pin, user_id))
        if display_name is not None:
            conn.execute("UPDATE users SET display_name = ? WHERE id = ?", (display_name, user_id))
        if is_admin is not None:
            conn.execute("UPDATE users SET is_admin = ? WHERE id = ?", (int(is_admin), user_id))
        if allowed_tabs is not None:
            conn.execute("UPDATE users SET allowed_tabs = ? WHERE id = ?", (allowed_tabs, user_id))
        conn.commit()
    finally:
        conn.close()


def delete_user(user_id):
    """Delete a user and their vehicle assignments."""
    conn = get_db()
    try:
        conn.execute("DELETE FROM users WHERE id = ?", (user_id,))
        conn.commit()
    finally:
        conn.close()


def get_user_vehicles(user_id):
    """Return list of vehicle_id strings assigned to a user."""
    conn = get_db()
    rows = conn.execute(
        "SELECT vehicle_id FROM user_vehicles WHERE user_id = ?", (user_id,)
    ).fetchall()
    conn.close()
    return [r["vehicle_id"] for r in rows]


def set_user_vehicles(user_id, vehicle_ids):
    """Replace all vehicle assignments for a user."""
    conn = get_db()
    try:
        conn.execute("DELETE FROM user_vehicles WHERE user_id = ?", (user_id,))
        for vid in vehicle_ids:
            conn.execute(
                "INSERT INTO user_vehicles (user_id, vehicle_id) VALUES (?, ?)",
                (user_id, vid),
            )
        conn.commit()
    finally:
        conn.close()
