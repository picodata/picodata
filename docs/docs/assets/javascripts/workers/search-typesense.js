/*
 * Поисковый воркер для MkDocs Material: ищет в Typesense, а при недоступности — встроенным lunr.
 *
 * Интерфейс поиска Material общается с воркером сообщениями (0 — настройка, 1 — готов,
 * 2 — запрос, 3 — результат), main.html подставляет этот файл вместо встроенного воркера и
 * передаёт параметры в адресе: ts — адрес Typesense, v — версия документации, lunr — имя
 * встроенного воркера, index — полный индекс для lunr. Ключа Typesense здесь нет: его
 * подставляет прокси перед Typesense, и он же пропускает запросы только со страниц документации.
 *
 * Встроенный воркер грузится только при первом сбое: его обработчик сообщений перехватывается,
 * и дальше запросы уходят в него. После сбоя Typesense не опрашивается RETRY_MS, чтобы каждый
 * символ в поле поиска не ждал таймаута.
 *
 * Запрос, по которому нашлось только «похожее», пробуется в другой раскладке клавиатуры:
 * «ыудусе» → «select».
 *
 * Выдача приходит в два приёма. Десять лучших страниц показываются сразу, на каждое нажатие.
 * Полный список — все страницы и все разделы на них — догружается, когда читатель перестал
 * печатать: на широких запросах это сотни килобайт и до секунды работы сервера.
 */
(() => {
  const params = new URL(self.location.href).searchParams;
  const TS_URL = params.get("ts");
  const VERSION = params.get("v") || "";
  const LUNR_WORKER = params.get("lunr");
  const FULL_INDEX = params.get("index");

  const TIMEOUT_MS = 1500;
  const RETRY_MS = 60000;
  // Полный список: 250 страниц и 99 разделов на страницу — наибольшее, что отдаёт Typesense.
  // В журнал запросов не попадает: тот же запрос уже записан первым обращением.
  const FULL = { per_page: 250, group_limit: 99, enable_highlight_v1: false, enable_analytics: false };
  const FULL_DELAY_MS = 300;
  const FULL_TIMEOUT_MS = 8000;
  // Метки подсветки из Typesense: текст экранируется целиком, потом метки становятся <mark>.
  const HL_START = "";
  const HL_END = "";

  // Typesense считает запрос законченным, когда читатель перестал печатать, и различает читателей
  // по этому идентификатору: без него все приходят с одного адреса прокси. Живёт до закрытия вкладки.
  const READER = Math.random().toString(36).slice(2) + Date.now().toString(36);

  const LAYOUT_EN = "`qwertyuiop[]asdfghjkl;'zxcvbnm,.";
  const LAYOUT_RU = "ёйцукенгшщзхъфывапролджэячсмитьбю";

  // Должно совпадать с индексатором (helm, docs-search/indexer.py).
  const collection = "docs_" + VERSION.replace(/[^A-Za-z0-9]+/g, "_");

  const realPost = self.postMessage.bind(self);
  let lunrSink = realPost;
  // Встроенный воркер отвечает через глобальный postMessage — направляем его ответы сюда.
  self.postMessage = (msg) => lunrSink(msg);

  let setup = null;
  let lunrHandler = null;
  let lunrReady = null;
  let tsDownUntil = VERSION ? 0 : Infinity;
  let seq = 0;
  let fullTimer = 0;
  let fullCtrl = null;

  function loadLunr() {
    if (lunrReady) return lunrReady;
    lunrReady = (async () => {
      const add = self.addEventListener;
      self.addEventListener = (type, fn, opts) => {
        if (type === "message") lunrHandler = fn;
        else add.call(self, type, fn, opts);
      };
      try {
        importScripts(LUNR_WORKER);
      } finally {
        self.addEventListener = add;
      }
      // Интерфейс получил урезанный индекс, lunr нужен полный.
      const data = { ...setup.data };
      if (FULL_INDEX) {
        const full = await (await fetch(FULL_INDEX)).json();
        data.config = full.config;
        data.docs = full.docs;
      }
      await new Promise((resolve) => {
        lunrSink = (msg) => {
          if (msg.type === 1) resolve();
        };
        lunrHandler({ data: { type: 0, data } });
      });
      lunrSink = realPost;
    })();
    return lunrReady;
  }

  function escapeHtml(s) {
    return s.replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);
  }

  const WORD = /[\p{L}\p{N}_]+/gu;
  const MARK_TAIL = new RegExp(HL_END + "([\\p{L}\\p{N}_]+)", "gu");
  const MARKED = new RegExp(HL_START + "([^" + HL_END + "]+)" + HL_END, "g");

  // Совпадение через синоним подсвечено только в поле как есть (title), через словоформу —
  // только в стеммированном (title_ru), причём там метка стоит на основе слова («Kubern|etes»).
  // Берём первое поле с подсветкой, дотягиваем метки до конца слов и добавляем слова,
  // подсвеченные в остальных полях.
  function highlighted(hit, fields, fallback) {
    const snippets = fields.map((f) => hit.highlight && hit.highlight[f] && hit.highlight[f].snippet)
      .filter((x) => x && x.includes(HL_START))
      .map((x) => x.replace(MARK_TAIL, "$1" + HL_END));
    if (!snippets.length) return fallback === undefined ? "" : escapeHtml(fallback);
    let s = snippets[0];
    const words = new Set();
    for (const other of snippets.slice(1)) {
      for (const m of other.matchAll(MARKED)) words.add(m[1].toLowerCase());
    }
    if (words.size) {
      s = s.replace(WORD, (w, at) => (words.has(w.toLowerCase()) && s[at - 1] !== HL_START ? HL_START + w + HL_END : w));
    }
    return escapeHtml(s).split(HL_START).join("<mark data-md-highlight>").split(HL_END).join("</mark>");
  }

  // null, если запрос набран вперемешку или без букв: тогда раскладка ни при чём.
  function otherLayout(query) {
    const lower = query.toLowerCase();
    const ru = /[а-яё]/.test(lower);
    const en = /[a-z]/.test(lower);
    if (ru === en) return null;
    const [from, to] = ru ? [LAYOUT_RU, LAYOUT_EN] : [LAYOUT_EN, LAYOUT_RU];
    return [...lower].map((c) => (from.includes(c) ? to[from.indexOf(c)] : c)).join("");
  }

  // Нашлось ли хоть одно слово запроса так, как оно набрано. Нечёткий поиск по «ыудусе» находит
  // «будущем», поэтому одного «что-то нашлось» мало.
  function matchesAsTyped(r, query) {
    const typed = query.toLowerCase().match(WORD) || [];
    return (r.grouped_hits || []).slice(0, 3).some((g) => Object.values(g.hits[0].highlight || {})
      .flatMap((h) => h.matched_tokens || []).flat()
      .some((m) => typed.some((t) => String(m).toLowerCase().startsWith(t.slice(0, 3)))));
  }

  // Возвращает ответ вместе с запросом, которым он получен: полный список просят им же.
  async function typesense(query) {
    const r = await request(query);
    const swapped = otherLayout(query);
    if (!swapped || matchesAsTyped(r, query)) return [r, query, {}];
    // Проба строгая (без опечаток) и не попадает в журнал запросов: её набирал не читатель.
    const strict = { num_typos: 0, enable_analytics: false };
    const probe = await request(swapped, strict);
    return probe.found > 0 ? [probe, swapped, strict] : [r, query, {}];
  }

  function truncated(r) {
    const groups = r.grouped_hits || [];
    return r.found > groups.length || groups.some((g) => g.found > g.hits.length);
  }

  function cancelFull() {
    clearTimeout(fullTimer);
    if (fullCtrl) fullCtrl.abort();
    fullCtrl = null;
  }

  function scheduleFull(id, query, extra) {
    fullTimer = setTimeout(async () => {
      const ctrl = (fullCtrl = new AbortController());
      try {
        const r = await request(query, { ...extra, ...FULL }, FULL_TIMEOUT_MS, ctrl);
        if (id === seq) realPost({ type: 3, data: toMaterial(r) });
      } catch (e) {
        // Первые результаты уже показаны, поэтому сбой здесь не повод уходить на встроенный поиск.
      }
    }, FULL_DELAY_MS);
  }

  async function request(query, extra, timeoutMs = TIMEOUT_MS, ctrl = new AbortController()) {
    const timer = setTimeout(() => ctrl.abort(), timeoutMs);
    try {
      // Строковое тело и параметры в адресе — «простой» CORS-запрос, без лишнего preflight.
      const res = await fetch(`${TS_URL}/multi_search?x-typesense-user-id=${READER}`, {
        method: "POST",
        body: JSON.stringify({
          searches: [{
            collection,
            q: query,
            preset: "docs",
            highlight_start_tag: HL_START,
            highlight_end_tag: HL_END,
            ...extra,
          }],
        }),
        signal: ctrl.signal,
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const r = (await res.json()).results[0];
      // multi_search отвечает 200, ошибка конкретного поиска (нет индекса версии) — внутри.
      if (r.error) throw new Error(r.error);
      return r;
    } finally {
      clearTimeout(timer);
    }
  }

  function toMaterial(r) {
    const groups = r.grouped_hits || [];
    const items = groups.map((g, gi) => {
      const score = groups.length - gi;
      const docs = g.hits.map((hit, i) => ({
        location: hit.document.location,
        title: highlighted(hit, ["title_ru", "title"], hit.document.title),
        text: highlighted(hit, ["text_ru"]),
        // Первое совпадение показывается, остальные разделы страницы — под «ещё N».
        score: i === 0 ? score : score - 0.5,
        terms: {},
      }));
      // Интерфейсу нужна сама страница в группе, даже если совпали только её разделы.
      if (!docs.some((d) => !d.location.includes("#"))) {
        const first = g.hits[0].document;
        docs.push({ location: first.page, title: escapeHtml(first.page_title), text: "", score: 0, terms: {} });
      }
      return docs;
    });
    return { items };
  }

  addEventListener("message", async (ev) => {
    const msg = ev.data;
    if (msg.type === 0) {
      setup = msg;
      realPost({ type: 1 });
      return;
    }
    if (msg.type !== 2) return;

    const id = ++seq;
    cancelFull();
    // Typesense читает дефис в начале слова как «исключить это слово»: «--memtx-memory» не нашёл бы
    // ничего, а «picodata run --peer» — страницы, где этого флага нет.
    const query = String(msg.data || "").replace(/(^|\s)-+/g, "$1").trim();
    if (!query) {
      realPost({ type: 3, data: { items: [] } });
      return;
    }
    if (Date.now() >= tsDownUntil) {
      try {
        const [r, asked, extra] = await typesense(query);
        // Ответы приходят не по порядку: устаревший не должен перекрыть свежий.
        if (id === seq) {
          realPost({ type: 3, data: toMaterial(r) });
          if (truncated(r)) scheduleFull(id, asked, extra);
        }
        return;
      } catch (e) {
        tsDownUntil = Date.now() + RETRY_MS;
        console.warn(`search: Typesense unavailable, using built-in search (${e})`);
      }
    }
    await loadLunr();
    if (id === seq) lunrHandler({ data: msg });
  });
})();
