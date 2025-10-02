from nltk.stem import WordNetLemmatizer
import re
import emoji

# Video classification by title

product_keywords = {
    'stationery': ['stationery'],
    'pens': ['pen', 'fountain pen', 'ballpoint pen', 'gel pen', 'multi pen', 'rollerball pen',
             'brush pen', 'calligraphy pen', 'comic pen', 'manga pen', 'dip pen', 'marker',
             'felt tip pen', 'highlighter', 'stylus pen'],

    'refills_and_inks': ['refill', 'ink', 'fountain pen ink', 'drawing ink', 'calligraphy ink',
                         'comic ink', 'dip pen ink', 'india ink', 'iron gall ink', 'shimmering ink',
                         'waterproof ink', 'nib', 'converter', 'bottle', 'blade', 'replacement', 'tip',
                         'inkwell', 'reservoir', 'cleaner', 'silicone grease', 'filler'],

    'pencils': ['pencil', 'mechanical pencil', 'drafting pencil', 'lead holder', 'lead pointer',
                'wooden pencil', 'pencil lead', 'pencil cap', 'pencil grip', 'pencil sharpener',
                'pencil holder', 'eraser'],

    'paper': ['paper', 'notebook', 'notebook cover', 'binder', 'folder', 'planner', 'journal',
              'sketchbook', 'sticky note', 'notepad', 'envelope', 'letter', 'flash card', 'index card',
              'postcard', 'loose leaf paper', 'comic paper'],

    'crafts': ['tape', 'glue', 'washi tape', 'tape runner', 'clear tape', 'stamp', 'stamp ink pad',
               'stamp cleaner', 'sticker', 'transfer sticker', 'sealing wax', 'wax seal stamp',
               'watercolor', 'acrylic', 'gouache', 'palette', 'coloring book', 'crayon', 'stencil', 'chalk',
               'tape cutter'],

    'cases_and_bags': ['pencil case', 'pen case', 'bag', 'backpack', 'pouch', 'purse', 'case'],

    'office_and_toys': ['bookmark', 'correction tape', 'correction pen', 'ruler', 'scissors', 'paper clip',
                        'desk organizer', 'desk tray', 'stapler', 'keychain', 'plushie', 'binder divider']
}

content_type = {
    'tutorial': ['tutorial', 'how to', 'guide', 'tip', 'diy', 'trick', 'way'],
    'review': ['review', 'unboxing'],
    'haul': ['haul', 'budget'],
    'showcase': ['collection', 'favorite', "what's in", 'techo kaigi']
}

brands = ['hobonichi', 'kokuyo']

def preprocess_title(title):
    """
    Extract useful info from title with emoji handling
    """

    title_no_emoji = emoji.replace_emoji(title, replace = '')

    emojis_found = emoji.emoji_list(title)

    return {
        'title_clean': title_no_emoji.strip(),
        'emojis': [e['emoji'] for e in emojis_found]
    }

def categorize_video(title):
    wnl = WordNetLemmatizer()
    title_lower = title.lower()
    words_in_title = title_lower.split()
    words_in_title = [wnl.lemmatize(word) for word in words_in_title]
    products = []
    content_types = []

    for category, keywords in product_keywords.items():
        if any(keyword in words_in_title for keyword in keywords):
            products.append(category)

    for category, keywords in content_type.items():
        if any(keyword in words_in_title for keyword in keywords):
            content_types.append(category)

    return {'products': products, 'content_types': content_types}