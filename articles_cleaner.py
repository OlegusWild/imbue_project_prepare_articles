"""
Different cleaning strategies
"""

import re


def get_cleaned_text(text: str):
    blocks = []

    for block in text.split('\n'):
        # most likely a good paragraph
        stripped_block = block.strip()
        if stripped_block and re.match(r'^[\w\"\']', stripped_block) and stripped_block[-1] in ('.', '?', '!', '\'', '"', '”', ')'):
            blocks.append(block)
    
    return '\n'.join(blocks)