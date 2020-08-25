"""Config

Distribute configuration

"""
import os


CONTAINER_BASE = os.getenv('CONTAINER_BASE', 'development')

GOB_OBJECTSTORE = 'GOBObjectstore'
