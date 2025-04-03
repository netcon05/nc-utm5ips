#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from __future__ import annotations
import os
import sys
import logging


# Used in logging module
APP_NAME: str = os.path.splitext(os.path.basename(sys.argv[0]))[0]
CUR_FOLDER: str = os.path.dirname(os.path.abspath(sys.argv[0]))

# Logging configuration section
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(threadName)s] [%(levelname)s] - %(message)s',
    handlers=[
        logging.FileHandler(f'{CUR_FOLDER}/{APP_NAME}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
