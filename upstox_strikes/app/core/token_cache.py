"""
This module provides a simple in-memory cache for the Upstox access token.
It is intended to be used by services to retrieve the token after 
it has been loaded from the database.
"""

# In-memory storage for the access token
upstox_access_token: str | None = None