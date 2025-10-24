# Product categories and classification rules
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

content_types = {
    'tutorial': ['tutorial', 'how to', 'guide', 'tip', 'diy', 'trick', 'way'],
    'review': ['review', 'unboxing'],
    'haul': ['haul', 'budget'],
    'showcase': ['collection', 'favorite', "what's in", 'techo kaigi'],
    'event': ['event', 'livestream', 'party', 'pen show', 'workshop', 'pop up', 'fest']
}

brands = {'paper': [
              'Hobonichi', 'Kokuyo', 'Midori', "TRAVELER'S COMPANY", 'Maruman',
              'Rhodia', 'Leuchtturm1917', 'Clairefontaine', 'Yamamoto', 'Stalogy',
              'LIFE', 'Tomoe River'],
          'fountain_pens': [
              'Pilot', 'Sailor', 'Platinum', 'LAMY', 'Kaweco', 'Pelikan', 'TWSBI',
              'Faber-Castell', 'Parker', 'BENU', 'Opus 88', 'Visconti'],
          'ink': [
              'Diamine', "Noodler's", 'Robert Oster', 'Herbin', 'Rohrer & Klingner',
              'De Atramentis', 'Colorverse', 'Takeda Jimuki', 'Dominant', 'Nagasawa', 'Monteverde'],
          'pencils_pens': [
              'Uni', 'Pentel', 'Zebra', 'Tombow', 'Sakura', 'Copic', 'Stabilo'],
          'art_supplies': [
              'Staedtler', 'Kuretake', 'Blackwing', 'Speedball', "Caran d'Ache",
              'Koh-I-Noor', 'Deleter', 'Tachikawa', 'Stillman & Birn', 'Winsor & Newton'],
          'bags': ['Lihit Lab', 'Doughnut', 'Sun-Star'],
          'featured_brands': [
              'JetPens', 'Rotring', 'TOOLS to LIVEBY', 'Sanby', 'Hightide', "Mark's",
              'Suatelier', 'Retro 51', 'BIGiDESIGN', 'Rickshaw', 'Kakimori', 'Field Notes'],
          'new_retailers': [
              'Green Flash', 'Sheaffer', 'Wearingeul', 'Cross', 'Clarto', 'Matsubokkuri',
              'OLFA', 'Girologic', 'UGears', 'Journalize', 'Greeting Life', 'Writech']
}


# Keywords for analyzing comment sentiment
positive_words = ['love', 'amazing', 'great', 'perfect', 'best', 'beautiful',
                  'satisfying', 'pretty', 'cute', 'nice', 'adorable', 'fantastic',
                  'excellent', 'wonderful', 'awesome', 'gorgeous', 'stunning']

negative_words = ['hate', 'terrible', 'bad', 'worst', 'disappointed', 'awful',
                  'horrible', 'ugly', 'poor', 'waste', 'regret', 'useless']

purchase_intent = ['where', 'buy', 'need', 'want', 'order', 'money', 'wallet',
                   'stock', 'available', 'price', 'request', 'restock', 'purchase', 'notification',
                   'sell', 'shop', 'store', 'link', 'amazon','shipping', 'delivery', 'checkout',
                   'cart', 'afford', 'expensive', 'cheap', 'deal', 'sale', 'discount']