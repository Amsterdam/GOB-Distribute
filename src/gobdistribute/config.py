"""Config

Distribute configuration

"""
import os


CONTAINER_BASE = os.getenv('CONTAINER_BASE', 'development')

GOB_OBJECTSTORE = 'GOBObjectstore'
EXPORT_API_HOST = os.getenv('EXPORT_API_HOST', 'http://localhost:8168')
