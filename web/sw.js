/* Tidal Echo — service worker (offline shell + Web Push).
   IMPORTANT: bump CACHE on every front-end change, or installed clients keep the
   old shell (the precached index.html won't refresh until the SW reinstalls). */
const AI_NAME = "穷奇";          // push-title fallback; keep in sync with index.html CONFIG.AI_NAME
const CACHE = "companion-v57-expand-older-btn";
const STICKER_CACHE = "companion-stickers-media-v1";
const PRECACHE = [
  "./index.html",
  "./broken_by_bunny.png",
  "./apple-touch-icon.png",
  "./favicon.png",
  "./icon-192.png",
  "./icon-512.png",
  "./chat_pink_bear.jpg",
  "./menu_pink_icecream.jpg",
  "./chat-light.webp", "./chat-harbor.webp",
  "./menu-light.webp", "./menu-harbor.webp",
  "./avatar-sea.png?v=2",
  "./silence.mp3",
];

self.addEventListener("install", (e) => {
  e.waitUntil(
    caches.open(CACHE)
      .then((c) => c.addAll(PRECACHE))
      .then(() => self.skipWaiting())
  );
});
self.addEventListener("activate", (e) => {
  e.waitUntil(
    caches.keys()
      .then((ks) => Promise.all(ks.filter((k) => k !== CACHE && k !== STICKER_CACHE).map((k) => caches.delete(k))))
      .then(() => self.clients.claim())
  );
});
self.addEventListener("fetch", (e) => {
  const url = new URL(e.request.url);
  if (url.pathname.startsWith("/relay/")) return;          // never intercept the API / SSE
  if (e.request.mode === "navigate") {
    // network-first for the page → an online reload always gets the latest index.html
    e.respondWith(fetch(e.request, { cache: "reload" }).catch(() => caches.match("./index.html")));
    return;
  }
  // 1. 同源静态资源优先走 App Shell 缓存
  if (e.request.method === "GET" && url.origin === location.origin) {
    e.respondWith(
      caches.match(e.request).then((r) => {
        if (r) return r;
        return fetch(e.request).then((res) => {
          const copy = res.clone();
          caches.open(CACHE).then((c) => c.put(e.request, copy));
          return res;
        });
      })
    );
    return;
  }
  // 2. 外部媒体（表情包图片、外部自定义大字体等）Cache-First 本地离线持久化缓存！
  // 无论是 postimg 表情包还是 neocities 等外部 ttf/woff 字体，首次下载后永久存入手机本地
  // 之后重开 PWA 0ms 瞬间秒开，永不重复耗费十几兆流量下载，断网离线依然可用
  const isMediaReq =
    e.request.destination === "image" ||
    e.request.destination === "font" ||
    /\.(png|jpe?g|gif|webp|svg|ttf|woff2?|otf|eot)(\?.*)?$/i.test(url.pathname);
  if (e.request.method === "GET" && isMediaReq) {
    e.respondWith(
      caches.open(STICKER_CACHE).then((cache) => {
        return cache.match(e.request).then((cachedResponse) => {
          if (cachedResponse) {
            return cachedResponse;
          }
          return fetch(e.request).then((networkResponse) => {
            if (networkResponse && (networkResponse.status === 200 || networkResponse.type === "opaque")) {
              cache.put(e.request, networkResponse.clone());
            }
            return networkResponse;
          }).catch(() => {
            return new Response("", { status: 404, statusText: "Media Offline" });
          });
        });
      })
    );
    return;
  }
});

// ── Web Push (VAPID) ──────────────────────────────
// The relay sends a push when the AI replies and no PWA tab is holding the stream;
// here we surface it on the lock screen.
self.addEventListener("push", (e) => {
  let d = {};
  try { d = e.data ? e.data.json() : {}; }
  catch (_) { d = { body: (e.data && e.data.text && e.data.text()) || "" }; }
  const title = d.title || AI_NAME;                        // backend sends RELAY_AI_NAME as title
  const body  = d.body  || "你有一条新消息";
  const tag   = d.id ? ("companion-" + d.id) : "companion-msg";
  e.waitUntil(
    self.registration.showNotification(title, {
      body,
      tag,
      renotify: true,
      icon:  "./icon-192.png",
      badge: "./icon-192.png",
      vibrate: [80, 40, 80],
      data: { url: d.url || "./" },
    })
  );
});

self.addEventListener("notificationclick", (e) => {
  e.notification.close();
  const target = (e.notification.data && e.notification.data.url) || "./";
  e.waitUntil(
    // matchAll only returns clients this SW controls (our own scope), so focus the first one.
    self.clients.matchAll({ type: "window", includeUncontrolled: true }).then((cls) => {
      for (const c of cls) {
        if ("focus" in c){ c.postMessage({ type: "backfill" }); return c.focus(); }
      }
      return self.clients.openWindow ? self.clients.openWindow(target) : null;
    })
  );
});
