# api/index.py
# Export a WSGI app for Vercel's Python runtime.
# We keep both `app` and `application` names to satisfy different loaders.
from app import app as application
app = application