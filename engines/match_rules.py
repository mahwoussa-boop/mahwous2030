"""قواعد المطابقة والمرادفات لمحرك مهووس"""
import re

REJECT_KEYWORDS = ["sample","╪╣┘è┘╪ر","╪╣┘è┘┘ç","decant","╪ز┘é╪│┘è┘à","split","miniature"]
KNOWN_BRANDS = [
    "Dior","Chanel","Gucci","Tom Ford","Versace","Armani","YSL","Prada","Burberry",
    "Hermes","Creed","Montblanc","Amouage","Rasasi","Lattafa","Arabian Oud","Ajmal",
    "Al Haramain","Afnan","Armaf","Mancera","Montale","Kilian","Jo Malone",
    "Carolina Herrera","Paco Rabanne","Mugler","Ralph Lauren","Parfums de Marly",
    "Nishane","Xerjoff","Byredo","Le Labo","Roja","Narciso Rodriguez",
    "Dolce & Gabbana","Valentino","Bvlgari","Cartier","Hugo Boss","Calvin Klein",
    "Givenchy","Lancome","Guerlain","Jean Paul Gaultier","Issey Miyake","Davidoff",
    "Coach","Michael Kors","Initio","Memo Paris","Maison Margiela","Diptyque",
    "Missoni","Juicy Couture","Moschino","Dunhill","Bentley","Jaguar",
    "Boucheron","Chopard","Elie Saab","Escada","Ferragamo","Fendi",
    "Kenzo","Lacoste","Loewe","Rochas","Roberto Cavalli","Tiffany",
    "Van Cleef","Azzaro","Chloe","Elizabeth Arden","Swiss Arabian",
    "Penhaligons","Clive Christian","Floris","Acqua di Parma",
    "Ard Al Zaafaran","Nabeel","Asdaaf","Maison Alhambra",
    "Tiziana Terenzi","Maison Francis Kurkdjian","Serge Lutens",
    "Frederic Malle","Ormonde Jayne","Zoologist","Tauer",
    "Banana Republic","Benetton","Bottega Veneta","Celine","Dsquared2",
    "Ermenegildo Zegna","Sisley","Mexx","Amadou","Thameen",
    "Nasomatto","Nicolai","Replica","Atelier Cologne","Aerin",
    "Angel Schlesser","Annick Goutal","Antonio Banderas","Balenciaga",
    "Bond No 9","Boadicea","Carner Barcelona","Clean","Commodity",
    "Costume National","Creed","Derek Lam","Diptique","Estee Lauder",
    "Franck Olivier","Giorgio Beverly Hills","Guerlain","Guess",
    "Histoires de Parfums","Illuminum","Jimmy Choo","Kenneth Cole",
    "Lalique","Lolita Lempicka","Lubin","Miu Miu","Moresque",
    "Nobile 1942","Oscar de la Renta","Oud Elite","Philipp Plein",
    "Police","Prada","Rasasi","Reminiscence","Salvatore Ferragamo",
    "Stella McCartney","Ted Lapidus","Ungaro","Vera Wang","Viktor Rolf",
    "Zadig Voltaire","Zegna","Ajwad","Club de Nuit","Milestone",
    "┘╪╖╪د┘╪ر","╪د┘╪╣╪▒╪ذ┘è╪ر ┘┘╪╣┘ê╪»","╪▒╪╡╪د╪│┘è","╪ث╪ش┘à┘","╪د┘╪ص╪▒┘à┘è┘","╪ث╪▒┘à╪د┘",
    "╪ث┘à┘ê╪د╪ش","┘â╪▒┘è╪»","╪ز┘ê┘à ┘┘ê╪▒╪»","╪»┘è┘ê╪▒","╪┤╪د┘┘è┘","╪║┘ê╪ز╪┤┘è","╪ذ╪▒╪د╪»╪د",
    "┘à┘è╪│┘ê┘┘è","╪ش┘ê╪│┘è ┘â┘ê╪ز┘ê╪▒","┘à┘ê╪│┘â┘è┘┘ê","╪»╪د┘┘ç┘è┘","╪ذ┘╪ز┘┘è",
    "┘â┘è┘╪▓┘ê","┘╪د┘â┘ê╪│╪ز","┘┘╪»┘è","╪د┘è┘┘è ╪╡╪╣╪ذ","╪د╪▓╪د╪▒┘ê",
    "┘â┘è┘┘è╪د┘","┘┘è╪┤╪د┘","╪▓┘è╪▒╪ش┘ê┘","╪ذ┘┘ç╪د┘┘è╪║┘ê┘╪▓","┘à╪د╪▒┘┘è","╪ش┘è╪▒┘╪د┘",
    "╪ز┘è╪▓┘è╪د┘╪د ╪ز╪▒┘è┘╪▓┘è","┘à╪د┘è╪▓┘ê┘ ┘╪▒╪د┘╪│┘è╪│","╪ذ╪د┘è╪▒┘è╪»┘ê","┘┘è ┘╪د╪ذ┘ê",
    "┘à╪د┘╪│┘è╪▒╪د","┘à┘ê┘╪ز╪د┘┘è","╪▒┘ê╪ش╪د","╪ش┘ê ┘à╪د┘┘ê┘","╪س┘à┘è┘","╪ث┘à╪د╪»┘ê",
    "┘╪د╪│┘ê┘à╪د╪ز┘ê","┘à┘è╪▓┘ê┘ ┘à╪د╪▒╪ش┘è┘╪د","┘┘è┘â┘ê┘╪د┘è",
    "╪ش┘è┘à┘è ╪ز╪┤┘ê","┘╪د┘┘è┘â","╪ذ┘ê┘┘è╪│","┘┘è┘â╪ز┘ê╪▒ ╪▒┘ê┘┘",
    "┘â┘┘ê┘è","╪ذ╪د┘┘╪│┘è╪د╪║╪د","┘à┘è┘ê ┘à┘è┘ê",
]
WORD_REPLACEMENTS = {}
MATCH_THRESHOLD = 85; HIGH_CONFIDENCE = 95; REVIEW_THRESHOLD = 75
PRICE_TOLERANCE = 5; TESTER_KEYWORDS = ["tester","╪ز╪│╪ز╪▒"]; SET_KEYWORDS = ["set","╪╖┘é┘à","┘à╪ش┘à┘ê╪╣╪ر"]










_SYN = {
    "eau de parfum":"edp","او دو بارفان":"edp","أو دو بارفان":"edp",
    "او دي بارفان":"edp","بارفان":"edp","parfum":"edp","perfume":"edp",
    "eau de toilette":"edt","او دو تواليت":"edt","أو دو تواليت":"edt",
    "تواليت":"edt","toilette":"edt","toilet":"edt",
    "eau de cologne":"edc","كولون":"edc","cologne":"edc",
    "extrait de parfum":"extrait","parfum extrait":"extrait",
    "ديور":"dior","شانيل":"chanel","شنل":"chanel","أرماني":"armani","ارماني":"armani",
    "جورجيو ارماني":"armani","فرساتشي":"versace","فيرساتشي":"versace",
    "غيرلان":"guerlain","توم فورد":"tom ford","تومفورد":"tom ford",
    "لطافة":"lattafa","لطافه":"lattafa",
    "أجمل":"ajmal","رصاصي":"rasasi","أمواج":"amouage","كريد":"creed",
    "ايف سان لوران":"ysl","سان لوران":"ysl","yves saint laurent":"ysl",
    "غوتشي":"gucci","قوتشي":"gucci","برادا":"prada","برادة":"prada",
    "بربري":"burberry","بيربري":"burberry","جيفنشي":"givenchy","جفنشي":"givenchy",
    "كارولينا هيريرا":"carolina herrera","باكو رابان":"paco rabanne",
    "نارسيسو رودريغيز":"narciso rodriguez","كالفن كلاين":"calvin klein",
    "هوجو بوس":"hugo boss","فالنتينو":"valentino","بلغاري":"bvlgari",
    "كارتييه":"cartier","لانكوم":"lancome","جو مالون":"jo malone",
    "سوفاج":"sauvage","بلو":"bleu","إيروس":"eros","ايروس":"eros",
    "وان ميليون":"1 million",
    "إنفيكتوس":"invictus","أفينتوس":"aventus","عود":"oud","مسك":"musk",
    "ميسوني":"missoni","جوسي كوتور":"juicy couture","موسكينو":"moschino",
    "دانهيل":"dunhill","بنتلي":"bentley","كينزو":"kenzo","لاكوست":"lacoste",
    "فندي":"fendi","ايلي صعب":"elie saab","ازارو":"azzaro",
    "فيراغامو":"ferragamo","شوبار":"chopard","بوشرون":"boucheron",
    "لانكم":"lancome","لانكوم":"lancome","جيفنشي":"givenchy","جيفانشي":"givenchy",
    "بربري":"burberry","بيربري":"burberry","بوربيري":"burberry",
    "فيرساتشي":"versace","فرزاتشي":"versace",
    "روبيرتو كفالي":"roberto cavalli","روبرتو كافالي":"roberto cavalli",
    "سلفاتوري":"ferragamo","سالفاتوري":"ferragamo",
    "ايف سان لوران":"ysl","ايف سانت لوران":"ysl",
    "هيرميس":"hermes","ارميس":"hermes","هرمز":"hermes",
    "كيليان":"kilian","كليان":"kilian",
    "نيشان":"nishane","نيشاني":"nishane",
    "زيرجوف":"xerjoff","زيرجوفف":"xerjoff",
    "بنهاليغونز":"penhaligons","بنهاليغون":"penhaligons",
    "مارلي":"parfums de marly","دي مارلي":"parfums de marly",
    "جيرلان":"guerlain","غيرلان":"guerlain","جرلان":"guerlain",
    "تيزيانا ترينزي":"tiziana terenzi","تيزيانا":"tiziana terenzi",
    "ناسوماتو":"nasomatto",
    "ميزون مارجيلا":"maison margiela","مارجيلا":"maison margiela","ربليكا":"replica",
    "نيكولاي":"nicolai","نيكولائي":"nicolai",
    "مايزون فرانسيس":"maison francis kurkdjian","فرانسيس":"maison francis kurkdjian",
    "بايريدو":"byredo","لي لابو":"le labo",
    "مانسيرا":"mancera","مونتالي":"montale","روجا":"roja",
    "جو مالون":"jo malone","جومالون":"jo malone",
    "ثمين":"thameen","أمادو":"amadou","امادو":"amadou",
    "انيشيو":"initio","إنيشيو":"initio","initio":"initio",
    "جيمي تشو":"jimmy choo","جيميتشو":"jimmy choo",
    "لاليك":"lalique","بوليس":"police",
    "فيكتور رولف":"viktor rolf","فيكتور اند رولف":"viktor rolf",
    "كلوي":"chloe","شلوي":"chloe",
    "بالنسياغا":"balenciaga","بالنسياجا":"balenciaga",
    "ميو ميو":"miu miu",
    "استي لودر":"estee lauder","استيلودر":"estee lauder",
    "كوتش":"coach","مايكل كورس":"michael kors",
    "رالف لورين":"ralph lauren","رالف لوران":"ralph lauren",
    "ايزي مياكي":"issey miyake","ايسي مياكي":"issey miyake",
    "دافيدوف":"davidoff","ديفيدوف":"davidoff",
    "دولشي اند غابانا":"dolce gabbana","دولتشي":"dolce gabbana","دولشي":"dolce gabbana",
    "جان بول غولتييه":"jean paul gaultier","غولتييه":"jean paul gaultier","غولتيه":"jean paul gaultier",
    "غوتييه":"jean paul gaultier","جان بول غوتييه":"jean paul gaultier","قوتييه":"jean paul gaultier","قولتييه":"jean paul gaultier",
    "مونت بلانك":"montblanc","مونتبلان":"montblanc",
    "موجلر":"mugler","موغلر":"mugler","تييري موجلر":"mugler",
    "كلوب دي نوي":"club de nuit","كلوب دنوي":"club de nuit",
    "مايلستون":"milestone",
    "سكاندل":"scandal","سكاندال":"scandal",
    " مل ":" ml ","ملي ":"ml "," ملي":" ml","مل ":"ml ",
    "ليتر":"l","لتر ":"l "," لتر":" l"," ليتر":" l",
    " جم ":"g","جرام":"g"," غرام":" g",
    # ── توحيد الحروف العربية ──
    "أ":"ا","إ":"ا","آ":"ا","ة":"ه","ى":"ي","ؤ":"و","ئ":"ي","ـ":"",
    # ── تهجئات بديلة لكلمات العطور (الأهم للمطابقة) ──
    "بيرفيوم":"edp","بيرفيومز":"edp","بارفيومز":"edp","برفان":"edp",
    "پارفيوم":"edp","پرفيوم":"edp","بارفيم":"edp",
    "تواليت":"edt","تواليتة":"edt","طواليت":"edt",
    "اكسترايت":"extrait","اكستريت":"extrait","اكسترييت":"extrait",
    "انتينس":"intense","انتانس":"intense","إنتنس":"intense",
    # ── تهجئات الماركات الإضافية ──
    "ايسينشيال":"essential","اسنشيال":"essential","ايسانشيال":"essential",
    "اسنشال":"essential","ايسنشال":"essential","ايسينشال":"essential",
    "سولييل":"soleil","سولايل":"soleil","سوليل":"soleil",
    "فلورال":"floral","فلورل":"floral","فلوريل":"floral",
    "سوفاج":"sauvage","سوفايج":"sauvage","سافاج":"sauvage",
    "بلو":"bleu","بلوو":"bleu",
    "ليبر":"libre","ليبرة":"libre",
    "اوريجينال":"original","أوريجينال":"original",
    "إكسترا":"extra","اكسترا":"extra",
    "انفيوجن":"infusion","انفيجن":"infusion","انفيوزن":"infusion",
    "ديليت":"delight","ديلايت":"delight",
    "نيوتر":"neutre","نيوتره":"neutre","نيوتير":"neutre",
    "بيور":"pure","بيوره":"pure","بيورة":"pure",
    "نوار":"noir","نوير":"noir",
    "روز":"rose","روس":"rose",
    "جاسمين":"jasmine","جازمين":"jasmine","ياسمين":"jasmine",
    "ميلانجي":"melange","ميلانج":"melange",
    "بريلوج":"prelude","برولوج":"prelude",
    "ريزيرف":"reserve","ريزيرفي":"reserve",
    "اميثست":"amethyst","اميثيست":"amethyst",
    "دراكار":"drakkar","دراكر":"drakkar",
    "نمروود":"nimrod","نمرود":"nimrod",
    "اوليفيا":"olivia","اوليفيه":"olivia",
    "ليجند":"legend","ليجاند":"legend",
    "سبورت":"sport","سبورتس":"sport",
    "بلاك":"black","بلك":"black",
    "وايت":"white","وايث":"white",
    "جولد":"gold","قولد":"gold",
    "سيلفر":"silver","سيلفير":"silver",
    "نايت":"night","نايث":"night",
    "داي":"day",
    # "دي":"day",   # محذوف: يدمّر "ديور"/"ديفيدوف" وغيرها (str.replace بدون حدود)
    # "او":"",      # محذوف: يحذف حرفين من أي كلمة تحتويهما
    # ── v26.0: مرادفات إضافية لزيادة الدقة ──
    # أحجام بديلة
    "٥٠":"50","٧٥":"75","١٠٠":"100","١٢٥":"125","١٥٠":"150","٢٠٠":"200",
    "٢٥٠":"250","٣٠٠":"300","٣٠":"30","٨٠":"80",
    # تركيزات إضافية
    "بارفيوم انتنس":"edp intense","انتنس":"intense","إنتنس":"intense",
    "ابسولو":"absolue","ابسوليو":"absolue","ابسوليوت":"absolute",
    "اكستريم":"extreme","اكسترييم":"extreme",
    "بريفيه":"prive","بريفي":"prive","privee":"prive","privé":"prive",
    "ليجير":"legere","ليجيره":"legere","légère":"legere",
    # ماركات ناقصة
    "توماس كوسمالا":"thomas kosmala","كوسمالا":"thomas kosmala",
    "روسيندو ماتيو":"rosendo mateu","ماتيو":"rosendo mateu",
    "بوديسيا":"boadicea","بواديسيا":"boadicea",
    "نوبيلي":"nobile","نوبيل":"nobile",
    "كارنر":"carner","كارنير":"carner",
    "اتيليه كولون":"atelier cologne","اتيليه":"atelier",
    "بوند نمبر ناين":"bond no 9","بوند":"bond",
    "هيستوار":"histoires","هيستوريز":"histoires",
    "لوبين":"lubin","لوبان":"lubin",
    "فيليب بلين":"philipp plein","فيلب بلين":"philipp plein",
    "اوسكار دي لا رنتا":"oscar de la renta","اوسكار":"oscar",
    "ستيلا مكارتني":"stella mccartney","ستيلا":"stella",
    "زاديغ":"zadig","زاديج":"zadig",
    "تيد لابيدوس":"ted lapidus","لابيدوس":"ted lapidus",
    "انقارو":"ungaro","اونغارو":"ungaro",
    "فيرا وانق":"vera wang","فيرا وانغ":"vera wang",
    "كينيث كول":"kenneth cole","كينث كول":"kenneth cole",
    "اد هاردي":"ed hardy","ايد هاردي":"ed hardy",
    # كلمات عطرية شائعة
    "عنبر":"amber","عنبري":"amber","امبر":"amber",
    "عود":"oud","عودي":"oud",
    "مسك":"musk","مسكي":"musk",
    "زعفران":"saffron","زعفراني":"saffron",
    "بخور":"incense","بخوري":"incense",
    "فانيلا":"vanilla","فانيليا":"vanilla",
    "باتشولي":"patchouli","باتشولي":"patchouli",
    "صندل":"sandalwood","صندلي":"sandalwood",
    "توباكو":"tobacco","تبغ":"tobacco",
    # تصحيح إملائي شائع
    "بيرفوم":"edp","بريفيوم":"edp","بارفوم":"edp",
    "تولت":"edt","تويلت":"edt",
}

_NOISE_RE = re.compile(
    r'\b(عطر|تستر|تيستر|tester|'
    r'بارفيوم|بيرفيوم|بارفيومز|بيرفيومز|برفيوم|برفان|بارفان|بارفيم|'
    r'تواليت|تواليتة|كولون|اكسترايت|اكستريت|اكسترييت|'
    r'او\s*دو|او\s*دي|أو\s*دو|أو\s*دي|'
    r'الرجالي|النسائي|للجنسين|رجالي|نسائي|'
    r'parfum|perfume|cologne|toilette|extrait|intense|'
    r'eau\s*de|pour\s*homme|pour\s*femme|for\s*men|for\s*women|unisex|'
    r'edp|edt|edc)\b'
    r'|\b\d+(?:\.\d+)?\s*(?:ml|مل|ملي|oz)\b'   # أحجام: 100ml, 50مل
    # حُذف: r'|\b(100|200|50|75|150|125|250|300|30|80)\b'
    # السبب: يحذف أرقاماً مهمة من أسماء المنتجات مثل "212 VIP" و "No. 5"
    , re.UNICODE | re.IGNORECASE
)

_CAP_VOL_RE = re.compile(
    r"(\d+(?:[.,]\d+)?)\s*(ml|milliliter|millilitres?|oz|ounce|ounces|مل|ملي)",
    re.IGNORECASE | re.UNICODE,
)

_BUNDLE_KW_RE = re.compile(
    r"(?:طقم|مجموعة|بكج|باكج|gift\s*set|طقم\s*هدايا|\bset\b|\bbundle\b|\bkit\b)",
    re.IGNORECASE | re.UNICODE,
)

