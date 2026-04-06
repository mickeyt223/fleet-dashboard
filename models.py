"""User database models for fleet dashboard authentication."""

import os
import sqlite3
from flask_login import UserMixin
from werkzeug.security import generate_password_hash, check_password_hash

DB_PATH = os.path.join(os.path.dirname(__file__), "fleet_users.db")


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
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            display_name TEXT,
            is_admin INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS user_vehicles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            vehicle_id TEXT NOT NULL,
            UNIQUE(user_id, vehicle_id)
        );
    """)

    # Seed default admin if no users exist
    count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    if count == 0:
        conn.execute(
            "INSERT INTO users (username, password_hash, display_name, is_admin) VALUES (?, ?, ?, ?)",
            ("admin", generate_password_hash("admin"), "Administrator", 1),
        )
        print("Default admin user created (username: admin, password: admin)")

    conn.commit()
    conn.close()


class User(UserMixin):
    """Flask-Login compatible user object."""

    def __init__(self, id, username, password_hash, display_name, is_admin):
        self.id = id
        self.username = username
        self.password_hash = password_hash
        self.display_name = display_name or username
        self.is_admin = bool(is_admin)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

    @staticmethod
    def get_by_id(user_id):
        conn = get_db()
        row = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
        conn.close()
        if row:
            return User(row["id"], row["username"], row["password_hash"],
                        row["display_name"], row["is_admin"])
        return None

    @staticmethod
    def get_by_username(username):
        conn = get_db()
        row = conn.execute("SELECT * FROM users WHERE username = ?", (username,)).fetchone()
        conn.close()
        if row:
            return User(row["id"], row["username"], row["password_hash"],
                        row["display_name"], row["is_admin"])
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
        "SELECT id, username, display_name, is_admin, created_at FROM users ORDER BY username"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def create_user(username, password, display_name, is_admin=False):
    """Create a new user. Returns user id."""
    conn = get_db()
    try:
        cur = conn.execute(
            "INSERT INTO users (username, password_hash, display_name, is_admin) VALUES (?, ?, ?, ?)",
            (username, generate_password_hash(password), display_name, int(is_admin)),
        )
        conn.commit()
        user_id = cur.lastrowid
    finally:
        conn.close()
    return user_id


def update_user(user_id, username=None, password=None, display_name=None, is_admin=None):
    """Update user fields. Only non-None values are changed."""
    conn = get_db()
    try:
        if username is not None:
            conn.execute("UPDATE users SET username = ? WHERE id = ?", (username, user_id))
        if display_name is not None:
            conn.execute("UPDATE users SET display_name = ? WHERE id = ?", (display_name, user_id))
        if is_admin is not None:
            conn.execute("UPDATE users SET is_admin = ? WHERE id = ?", (int(is_admin), user_id))
        if password is not None:
            conn.execute("UPDATE users SET password_hash = ? WHERE id = ?",
                         (generate_password_hash(password), user_id))
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
