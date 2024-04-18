"""
Different cleaning strategies
"""

import re
import nltk


nltk.download('punkt')


def get_cleaned_text(text: str):
    blocks = []

    for block in text.split('\n'):
        # # most likely a good paragraph
        # stripped_block = block.strip()
        # if stripped_block and re.match(r'^[\w\"\']', stripped_block) and stripped_block[-1] in ('.', '?', '!', '\'', '"', '”', ')'):
        #     blocks.append(block)
        good_sentences = []
        for sentence in nltk.sent_tokenize(block):
            sentence = remove_unwanted(sentence)
            if sentence:
                good_sentences.append(sentence)
        if good_sentences:
            blocks.append(' '.join(good_sentences))
    
    return '\n'.join(blocks)


# borrowed from stackoverflow https://stackoverflow.com/a/49146722
def remove_emoji(string):
    emoji_pattern = re.compile("["
                           u"\U0001F600-\U0001F64F"  # emoticons
                           u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                           u"\U0001F680-\U0001F6FF"  # transport & map symbols
                           u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           u"\U00002702-\U000027B0"
                           u"\U000024C2-\U0001F251"
                           "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r' ', string)

def remove_unwanted(sentence: str):
    if sentence[-1] not in ('.', '?', '!', '\'', '"', '”', ')'):
        return ''
    # remove user mentions
    sentence = re.sub("@[A-Za-z0-9_]+"," ", sentence)
    # remove URLS
    sentence = re.sub(r'http\S+', ' ', sentence)
    # remove hashtags
    sentence = re.sub("#[A-Za-z0-9_]+","", sentence)
    # remove emoji's
    sentence = remove_emoji(sentence)
    # remove double spaces
    sentence = sentence.replace('  '," ")
    
    return sentence.strip()