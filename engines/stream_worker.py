"""
engines/stream_worker.py - V26 Elite Streaming Architecture
════════════════════════════════════════════════════════════
محرك التدفق عالي الأداء (High-Throughput Streaming Engine).
يقوم بربط الكاشف (Scraper) ومحرك المطابقة (Matcher) كمنتج ومستهلك (Producer/Consumer).
يشمل بوابات الفلترة الصارمة لمنع هلوسة الذكاء الاصطناعي، وقاطع الدائرة لمنع استنزاف الموارد.
"""
import asyncio
import logging
import re
import time
from typing import Any, Dict, List, Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("StreamWorker")

class StreamFilterGate:
    """
    بوابة الفلترة الصلبة (Hard-Gate Firewall)
    الهدف: منع مقارنة المنتجات غير المتوافقة جذرياً لمنع ההالوسة (Tester matched to Retail)
    وتقليل الاعتماد على LLM والبحث المتجهي (Vector Search) لتسريع المعالجة وتحقيق وقت O(1).
    """

    TESTER_REGEX = re.compile(r'\b(tester|تستر|عينة|sample|decant|تقسيم)\b', re.IGNORECASE)
    
    CONC_MAP = {
        'edp': re.compile(r'\b(edp|eau de parfum|او دو بارفان|او دي بارفان|بارفان|parfum)\b', re.IGNORECASE),
        'edt': re.compile(r'\b(edt|eau de toilette|او دو تواليت|تواليت|toilette)\b', re.IGNORECASE),
        'edc': re.compile(r'\b(edc|eau de cologne|كولونيا|cologne|كولون)\b', re.IGNORECASE),
        'extrait': re.compile(r'\b(extrait|parfum extrait|اكسترايت|اكستريت)\b', re.IGNORECASE),
        'intense': re.compile(r'\b(intense|انتنس|انتينس)\b', re.IGNORECASE)
    }

    SIZE_REGEX = re.compile(r'\b(\d{2,3})\s*(ml|مل|ملي)\b', re.IGNORECASE)

    @classmethod
    def extract_features(cls, name: str) -> Dict[str, Any]:
        """فحص بصمة المنتج من اسمه لاستخراج الخصائص الثابتة."""
        features = {
            'is_tester': bool(cls.TESTER_REGEX.search(name)),
            'concentration': None,
            'size_ml': None
        }
        
        # استخراج التركيز
        for conc_name, pattern in cls.CONC_MAP.items():
            if pattern.search(name):
                features['concentration'] = conc_name
                break
                
        # استخراج الحجم
        size_match = cls.SIZE_REGEX.search(name)
        if size_match:
            try:
                features['size_ml'] = int(size_match.group(1))
            except ValueError:
                pass
                
        return features

    @classmethod
    def is_compatible(cls, our_features: Dict[str, Any], comp_features: Dict[str, Any]) -> bool:
        """
        O(1) Logic Gate.
        يرجع False إذا كان المنتجان غير متوافقين بشكل قاطع، وهذا يحمي محرك المتجهات.
        """
        # 1. بوابة التستر: يمنع مطابقة عطر تستر مع عطر تجزئة
        if our_features['is_tester'] != comp_features['is_tester']:
            return False
            
        # 2. بوابة التركيز: يجب أن يتطابق التركيز (EDP مع EDP) إذا توفر في كلا الاسمين
        if our_features['concentration'] and comp_features['concentration']:
            if our_features['concentration'] != comp_features['concentration']:
                return False
                
        # 3. بوابة الحجم: تجاوز الفروق الطفيفة (مثل 90مل وطُبعت 100مل), لكن نرفض الفوارق الكبيرة جداً
        if our_features['size_ml'] and comp_features['size_ml']:
            if abs(our_features['size_ml'] - comp_features['size_ml']) > 15:
                return False
                
        return True


class AsyncCircuitBreaker:
    """قاطع الدائرة لحماية نظامك من الحظر أو الاستنزاف ( Cloudflare / 429 Errors)."""
    def __init__(self, max_failures: int = 5, reset_timeout: float = 60.0):
        self.max_failures = max_failures
        self.reset_timeout = reset_timeout
        self.failures = 0
        self.last_failure_time = 0.0
        self.is_open = False
        
    def record_failure(self):
        """تسجيل فشل (مثلاً 429 من Cloudflare أو 500 Timeout)."""
        self.failures += 1
        self.last_failure_time = time.time()
        if self.failures >= self.max_failures:
            self.is_open = True
            logger.warning(f"Circuit Breaker OPENED! Waiting {self.reset_timeout}s to cool off IP.")
            
    def record_success(self):
        """تصفير العداد عند نجاح الطلب."""
        self.failures = 0
        self.is_open = False
        
    async def wait_if_open(self):
        """الانتظار الإجباري إذا كانت الدائرة مفتوحة، لمنع القصف بحظر دائم."""
        if self.is_open:
            elapsed = time.time() - self.last_failure_time
            if elapsed < self.reset_timeout:
                wait_time = self.reset_timeout - elapsed
                logger.info(f"Circuit OPEN. Suspending async task for {wait_time:.2f}s...")
                await asyncio.sleep(wait_time)
            
            # تجربة حذرة (Half-Open) للسماح بطلبات جديدة تدريجياً
            self.is_open = False
            self.failures = max(0, self.failures - 1)


async def match_consumer_worker(worker_id: int, stream_queue: asyncio.Queue, vector_index=None):
    """
    عامل استهلاك المطابقة (Matcher Consumer).
    يسحب المنتجات فور كشطها (Streaming) ويطابقها في ثوانٍ دون انتظار انتهاء المتجر بالكامل.
    """
    logger.info(f"[Matcher Worker {worker_id}] Started listening to stream queue.")
    
    while True:
        try:
            item = await stream_queue.get()
            
            # إشارة إيقاف من الـ Producer
            if item is None:
                stream_queue.task_done()
                logger.info(f"[Matcher Worker {worker_id}] Received STOP signal.")
                break
                
            comp_name = str(item.get('اسم المنتج', ''))
            
            # 1. استخراج بصمة المنافس
            comp_features = StreamFilterGate.extract_features(comp_name)
            
            # TODO: 2. HNSW Vector Search (يتم دمج FAISS أو ScaNN هنا)
            # candidates = vector_index.search(...)
            candidates = [] # قائمة المرشحين التجريبية
            await asyncio.sleep(0.01) # محاكاة زمن الاسترداد (<10ms)
            
            # 3. دفع المرشحين عبر بوابة الفلترة (Hard-Gate)
            valid_matches = []
            for cand in candidates:
                our_features = StreamFilterGate.extract_features(cand['اسم المنتج'])
                if StreamFilterGate.is_compatible(our_features, comp_features):
                    valid_matches.append(cand)
                    
            # 4. إذا نجا المنتج من الفلترة وكان متطابقاً بقوة (>99% أوผ่าน Gemini)
            if valid_matches:
                # يمرر التوجيه للذكاء الاصطناعي، أو يحفظ مباشرة كنجاح (Tier 1/2) 
                logger.debug(f"[Matcher {worker_id}] Matched explicitly: {comp_name}")
                pass
            
            # إنهاء المهمة للطابور
            stream_queue.task_done()
            
        except asyncio.CancelledError:
            logger.info(f"[Matcher Worker {worker_id}] Task processing Cancelled.")
            break
        except Exception as e:
            logger.error(f"[Matcher Worker {worker_id}] Error in consumer pipe: {e}")
            stream_queue.task_done()


async def run_streaming_orchestrator(producer_coroutine, num_consumers: int = 5):
    """
    عصب المعمارية (Streaming Orchestrator).
    يربط منتجي البيانات (Scrapers) مع مجموعة من المستهلكين المنفصلين في الوقت الحقيقي.
    """
    # Queue بسعة 3000 لمنع تكدس الذاكرة وحدوث OOM على RailWay
    product_stream_queue = asyncio.Queue(maxsize=3000)
    
    consumers = []
    # 1. تشغيل عمال المطابقة في الخلفية للانتظار
    for i in range(num_consumers):
        task = asyncio.create_task(match_consumer_worker(i, product_stream_queue))
        consumers.append(task)
        
    logger.info("Streaming Orchestrator: Both Scraper Producers & Matcher Consumers are ALIVE.")
    
    # 2. تشغيل الكاشف وإعطائه الأنبوب ليرمي فيه المنتجات بمجرد العثور عليها
    producer_task = asyncio.create_task(producer_coroutine(product_stream_queue))
    
    # 3. الانتظار المجدول حتى ينتهي الكاشط بالكامل من سحب كل الروابط
    await producer_task
    
    # 4. بعد انتهاء الكشط: إرسال "Poison Pill" (None) لكل عامل لإيقافه بأمان
    for _ in range(num_consumers):
        await product_stream_queue.put(None)
        
    # 5. تنظيف الذاكرة والانتظار حتى إنهاء معالجة المنتجات المتبقية في الأنبوب
    await product_stream_queue.join()
    await asyncio.gather(*consumers)
    
    logger.info("Streaming Orchestrator: Pipeline Completed Successfully.")
